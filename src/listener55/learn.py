"""Learned per-service baselines.

The fleet bus used one fixed staleness window for every service. That is wrong
in both directions: a service that pushes every 5s is dead long before 90s
pass, and one that pushes every 5 minutes is never "fresh" at all. This module
learns what normal looks like for each reporter and lets the bus judge it
against its own history instead of a global constant.

What is learned, per service key:

*   **cadence** -- an exponentially weighted mean and mean-absolute-deviation
    of the gap between reports. The stale threshold becomes
    ``tolerance * interval + 4 * deviation``, clamped to a floor and ceiling.
*   **tolerance** -- starts at 3 missed pushes. Each alarm that turns out to be
    a late report rather than an outage widens it; real outages let it relax
    back. A chronically jittery service stops paging, a steady one stays tight.
*   **restarts** -- a new ``started_at`` the bus has not seen recently. Three
    inside ten minutes is a crash loop, which a liveness check alone reads as
    "healthy" every time it looks.
*   **error rates** -- for counters that look like errors, the normal rate of
    increase, so a spike is judged against that service's own noise floor.
*   **presence** -- a peer that has reported often enough is remembered, so
    when it goes quiet the bus notices even though no registry lists it, and
    is forgotten again after a long silence so it cannot alarm forever.

All of it is plain arithmetic on the reports the bus already receives. No
model is embedded here: the service stays usable with nothing but the stdlib,
and whatever agent drives it brings its own reasoning.
"""

from __future__ import annotations

import re
import threading
import time
from typing import Any, Callable

ALPHA = 0.2  # cadence EWMA weight
RATE_ALPHA = 0.1  # counter-rate EWMA weight
DEFAULT_TOLERANCE = 3.0
MIN_TOLERANCE = 2.0
MAX_TOLERANCE = 8.0
MIN_RATE_SAMPLES = 8
SPIKE_MIN_DELTA = 3
FLAP_WINDOW_SECONDS = 600.0
FLAP_RESTARTS = 3
MAX_COUNTERS = 32  # a peer cannot make the bus track unbounded series

_ERRORISH = re.compile(r"(error|fail|exception|timeout|dropped|rejected)", re.I)


def is_error_counter(name: str) -> bool:
    return bool(_ERRORISH.search(name))


class Baselines:
    """Per-service learned statistics, persisted through a StateStore section."""

    def __init__(
        self,
        section: dict[str, Any],
        touch: Callable[[], None],
        *,
        enabled: bool = True,
        floor: float = 15.0,
        ceiling: float = 3600.0,
        min_samples: int = 5,
        roster_after: int = 20,
        forget_after: float = 86400.0,
        wall: Callable[[], float] = time.time,
    ) -> None:
        self._s = section
        self._touch = touch
        self.enabled = enabled
        self.floor = floor
        self.ceiling = ceiling
        self.min_samples = min_samples
        self.roster_after = roster_after
        self.forget_after = forget_after
        self._wall = wall
        self._lock = threading.Lock()
        # Monotonic arrival times are meaningless across a restart, so they
        # live here and not in the persisted section.
        self._last_mono: dict[str, float] = {}

    # ------------------------------------------------------------- observe

    def _entry(self, key: str) -> dict[str, Any]:
        e = self._s.get(key)
        if not isinstance(e, dict):
            e = {
                "reports": 0,
                "intervals": 0,
                "interval": None,
                "dev": 0.0,
                "tol": DEFAULT_TOLERANCE,
                "first_seen": self._wall(),
                "last_seen": None,
                "started": [],
                "restarts": [],
                "rates": {},
                "counts": {},
                "false_alarms": 0,
                "real_outages": 0,
            }
            self._s[key] = e
        return e

    def observe(self, key: str, report: dict[str, Any], now_mono: float) -> list[dict[str, Any]]:
        """Fold one accepted report into the baseline. Returns signals."""
        signals: list[dict[str, Any]] = []
        now_wall = self._wall()
        with self._lock:
            e = self._entry(key)
            prev_mono = self._last_mono.get(key)
            gap = (now_mono - prev_mono) if prev_mono is not None else None
            self._last_mono[key] = now_mono

            # --- cadence. A gap longer than the current threshold was an
            # outage, not a sample of the service's rhythm; learning from it
            # would teach the bus to tolerate the very thing it should flag.
            if gap is not None and gap > 0:
                limit = self._threshold_locked(e)
                if limit is None or gap <= limit:
                    if e["interval"] is None:
                        e["interval"] = gap
                    else:
                        e["dev"] = (1 - ALPHA) * e["dev"] + ALPHA * abs(gap - e["interval"])
                        e["interval"] = (1 - ALPHA) * e["interval"] + ALPHA * gap
                    e["intervals"] += 1

            # --- restarts.
            started = report.get("started_at")
            if isinstance(started, str) and started:
                seen = e["started"]
                if seen and started not in seen:
                    e["restarts"].append(now_wall)
                    e["restarts"] = e["restarts"][-20:]
                    recent = [t for t in e["restarts"] if now_wall - t <= FLAP_WINDOW_SECONDS]
                    signals.append({"kind": "restart", "recent_restarts": len(recent)})
                    if len(recent) >= FLAP_RESTARTS:
                        signals.append(
                            {
                                "kind": "crash_loop",
                                "restarts": len(recent),
                                "window_seconds": FLAP_WINDOW_SECONDS,
                            }
                        )
                if started not in seen:
                    # A few identities are kept so several instances sharing a
                    # key do not read as one instance restarting endlessly.
                    seen.append(started)
                    e["started"] = seen[-4:]

            # --- error-rate spikes.
            counts = report.get("counts")
            if isinstance(counts, dict):
                restarted = any(s["kind"] == "restart" for s in signals)
                for name, value in counts.items():
                    if not isinstance(value, int) or isinstance(value, bool):
                        continue
                    if not is_error_counter(name):
                        continue
                    if name not in e["counts"] and len(e["counts"]) >= MAX_COUNTERS:
                        continue
                    prev = e["counts"].get(name)
                    e["counts"][name] = value
                    if prev is None or gap is None or gap <= 0 or restarted:
                        continue
                    delta = value - prev
                    if delta < 0:
                        continue  # counter reset
                    rate = delta / gap
                    st = e["rates"].setdefault(name, {"mean": 0.0, "dev": 0.0, "n": 0})
                    if st["n"] >= MIN_RATE_SAMPLES and delta >= SPIKE_MIN_DELTA:
                        noise = max(st["dev"], 0.05 * st["mean"], 1e-9)
                        if rate > st["mean"] + 6 * noise:
                            signals.append(
                                {
                                    "kind": "error_spike",
                                    "counter": name,
                                    "delta": delta,
                                    "rate_per_second": round(rate, 4),
                                    "baseline_rate_per_second": round(st["mean"], 4),
                                }
                            )
                    st["dev"] = (1 - RATE_ALPHA) * st["dev"] + RATE_ALPHA * abs(rate - st["mean"])
                    st["mean"] = (1 - RATE_ALPHA) * st["mean"] + RATE_ALPHA * rate
                    st["n"] += 1

            e["reports"] += 1
            e["last_seen"] = now_wall
        self._touch()
        return signals

    # ----------------------------------------------------------- threshold

    def _threshold_locked(self, e: dict[str, Any]) -> float | None:
        if not self.enabled or e["interval"] is None or e["intervals"] < self.min_samples:
            return None
        raw = e["tol"] * e["interval"] + 4.0 * e["dev"]
        return max(self.floor, min(self.ceiling, raw))

    def threshold(self, key: str, default: float) -> tuple[float, str]:
        """(seconds, source) -- source is ``learned`` or ``configured``."""
        with self._lock:
            e = self._s.get(key)
            if isinstance(e, dict):
                learned = self._threshold_locked(e)
                if learned is not None:
                    return learned, "learned"
        return default, "configured"

    def feedback(self, key: str, real: bool) -> float | None:
        """Teach the baseline whether a staleness alarm was a real outage.

        A false alarm widens the tolerance by a quarter. A real outage pulls
        it a tenth of the way back to the default, so one noisy week does not
        leave a service permanently under-watched.
        """
        with self._lock:
            e = self._s.get(key)
            if not isinstance(e, dict):
                return None
            if real:
                e["real_outages"] += 1
                e["tol"] = DEFAULT_TOLERANCE + (e["tol"] - DEFAULT_TOLERANCE) * 0.9
            else:
                e["false_alarms"] += 1
                e["tol"] = min(MAX_TOLERANCE, e["tol"] * 1.25)
            e["tol"] = round(max(MIN_TOLERANCE, e["tol"]), 4)
            tol = e["tol"]
        self._touch()
        return tol

    # -------------------------------------------------------------- roster

    def learned_ids(self) -> list[str]:
        """Peers seen often enough, and recently enough, to be expected."""
        now = self._wall()
        with self._lock:
            return [
                k
                for k, e in self._s.items()
                if isinstance(e, dict)
                and e.get("reports", 0) >= self.roster_after
                and e.get("last_seen") is not None
                and now - e["last_seen"] < self.forget_after
            ]

    def prune(self) -> list[str]:
        """Drop baselines for peers silent longer than ``forget_after``."""
        now = self._wall()
        with self._lock:
            gone = [
                k
                for k, e in self._s.items()
                if isinstance(e, dict)
                and e.get("last_seen") is not None
                and now - e["last_seen"] >= self.forget_after
            ]
            for k in gone:
                del self._s[k]
                self._last_mono.pop(k, None)
        if gone:
            self._touch()
        return gone

    def forget(self, key: str) -> bool:
        with self._lock:
            existed = self._s.pop(key, None) is not None
            self._last_mono.pop(key, None)
        if existed:
            self._touch()
        return existed

    # --------------------------------------------------------------- query

    def restarts_since(self, key: str, since_wall: float) -> int:
        with self._lock:
            e = self._s.get(key)
            if not isinstance(e, dict):
                return 0
            return sum(1 for t in e["restarts"] if t >= since_wall)

    def describe(self, key: str) -> dict[str, Any] | None:
        with self._lock:
            e = self._s.get(key)
            if not isinstance(e, dict):
                return None
            learned = self._threshold_locked(e)
            now = self._wall()
            return {
                "reports_observed": e["reports"],
                "interval_seconds": round(e["interval"], 3) if e["interval"] is not None else None,
                "interval_deviation_seconds": round(e["dev"], 3),
                "tolerance": e["tol"],
                "learned_stale_after_seconds": round(learned, 3) if learned is not None else None,
                "samples_until_learned": max(0, self.min_samples - e["intervals"]),
                "remembered": e["reports"] >= self.roster_after,
                "restarts_last_10m": sum(
                    1 for t in e["restarts"] if now - t <= FLAP_WINDOW_SECONDS
                ),
                "restarts_total_tracked": len(e["restarts"]),
                "false_alarms": e["false_alarms"],
                "real_outages": e["real_outages"],
                "error_rates": {
                    name: {
                        "mean_per_second": round(st["mean"], 5),
                        "deviation": round(st["dev"], 5),
                        "samples": st["n"],
                    }
                    for name, st in e["rates"].items()
                },
            }

    def keys(self) -> list[str]:
        with self._lock:
            return [k for k, e in self._s.items() if isinstance(e, dict)]

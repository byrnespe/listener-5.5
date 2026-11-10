"""Self-report delivery: JSON POST and/or local JSONL log."""

from __future__ import annotations

import json
import logging
import threading
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Callable

from listener55 import __version__
from listener55.heal import CircuitBreaker
from listener55.schema import Metrics, assert_valid

#: Undelivered reports kept for replay. At the default 30s cadence this is
#: 100 minutes of sink outage backfilled with no gap; beyond that the oldest
#: are dropped, because an unbounded buffer is its own outage.
SPOOL_MAX = 200
#: Backlog replayed per cycle, so catching up never starves the live report.
REPLAY_PER_CYCLE = 25

log = logging.getLogger("listener55.reporter")


class SelfReporter:
    """Periodically emit validated self-report payloads."""

    def __init__(
        self,
        *,
        metrics: Metrics,
        instance_id: str,
        host: str,
        port: int,
        interval_seconds: float,
        report_url: str | None = None,
        report_log_path: Path | None = None,
        http_timeout: float = 5.0,
        post_fn: Callable[[str, bytes, float], tuple[int, str]] | None = None,
        breaker: CircuitBreaker | None = None,
        spool: list[str] | None = None,
        on_spool_change: Callable[[], None] | None = None,
        replay: bool = True,
        meta_fn: Callable[[], dict[str, Any]] | None = None,
    ) -> None:
        self.metrics = metrics
        self.instance_id = instance_id
        self.host = host
        self.port = port
        self.interval_seconds = interval_seconds
        self.report_url = report_url
        self.report_log_path = report_log_path
        self.http_timeout = http_timeout
        self._post_fn = post_fn or self._default_post
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._wanted = False
        self.restarts = 0
        self.breaker = breaker or CircuitBreaker(base_delay=max(1.0, interval_seconds))
        # The spool is a plain list so it can be a section of the state store
        # and survive a restart along with everything else that was learned.
        self._spool: list[str] = spool if spool is not None else []
        self._spool_lock = threading.Lock()
        self._on_spool_change = on_spool_change
        self.replay = replay
        self.replayed = 0
        self.spool_dropped = 0
        self._meta_fn = meta_fn

    def build(self) -> dict[str, Any]:
        meta: dict[str, Any] | None = None
        if self._meta_fn is not None:
            try:
                meta = self._meta_fn()
            except Exception:  # noqa: BLE001 -- meta is garnish; never lose a report over it
                log.exception("self-report meta builder crashed")
        payload = self.metrics.build_payload(
            instance_id=self.instance_id,
            host=self.host,
            port=self.port,
            meta=meta,
        )
        return assert_valid(payload)

    # ---------------------------------------------------------------- spool

    def spooled(self) -> int:
        with self._spool_lock:
            return len(self._spool)

    def _spool_add(self, body: bytes) -> None:
        if not self.replay:
            return
        with self._spool_lock:
            self._spool.append(body.decode("utf-8"))
            overflow = len(self._spool) - SPOOL_MAX
            if overflow > 0:
                del self._spool[:overflow]
                self.spool_dropped += overflow
        if self._on_spool_change:
            self._on_spool_change()

    def _replay_backlog(self) -> str | None:
        """Deliver spooled reports oldest first. Returns an error on the first
        failure, leaving the rest queued; None when the backlog is clear or
        the per-cycle budget is spent."""
        sent = 0
        while sent < REPLAY_PER_CYCLE:
            with self._spool_lock:
                if not self._spool:
                    break
                body = self._spool[0]
            try:
                status, _ = self._post_fn(self.report_url, body.encode("utf-8"), self.http_timeout)
            except (urllib.error.URLError, TimeoutError, OSError, ValueError) as exc:
                return f"http:{exc}"
            if status < 200 or status >= 300:
                if 400 <= status < 500 and status not in (408, 429):
                    # The sink will never take this one; do not let a single
                    # poison report block every report behind it.
                    with self._spool_lock:
                        if self._spool and self._spool[0] == body:
                            self._spool.pop(0)
                            self.spool_dropped += 1
                    continue
                return f"http status {status}"
            with self._spool_lock:
                if self._spool and self._spool[0] == body:
                    self._spool.pop(0)
            sent += 1
            self.replayed += 1
        if sent and self._on_spool_change:
            self._on_spool_change()
        return None

    def delivery(self) -> dict[str, Any]:
        return {
            "url_configured": bool(self.report_url),
            "log_configured": self.report_log_path is not None,
            "breaker": self.breaker.describe(),
            "spooled": self.spooled(),
            "replayed": self.replayed,
            "spool_dropped": self.spool_dropped,
            "thread_alive": self.alive(),
            "thread_restarts": self.restarts,
        }

    def emit_once(self) -> dict[str, Any]:
        payload = self.build()
        body = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
        ok = True
        errors: list[str] = []

        if self.report_log_path is not None:
            try:
                self._write_log(payload)
            except OSError as exc:
                ok = False
                errors.append(f"log:{exc}")

        if self.report_url:
            if not self.breaker.allow():
                # Sink is known-down: do not spend a timeout on it every
                # cycle. Keep the report for when it comes back.
                ok = False
                errors.append("http: circuit open, report spooled")
                self._spool_add(body)
            else:
                err = self._replay_backlog()
                if err is None:
                    try:
                        status, _ = self._post_fn(self.report_url, body, self.http_timeout)
                        if status < 200 or status >= 300:
                            err = f"http status {status}"
                    except (urllib.error.URLError, TimeoutError, OSError, ValueError) as exc:
                        err = f"http:{exc}"
                if err is None:
                    self.breaker.record_success()
                else:
                    ok = False
                    errors.append(err)
                    self.breaker.record_failure()
                    self._spool_add(body)

        if ok:
            self.metrics.mark_report_sent()
            log.info(
                "self-report ok status=%s uptime=%.1fs received=%s",
                payload["status"],
                payload["uptime_seconds"],
                payload["counts"]["received"],
            )
        else:
            msg = "; ".join(errors) if errors else "unknown"
            self.metrics.mark_report_failed(msg)
            log.warning("self-report failed: %s", msg)
        return payload

    def _write_log(self, payload: dict[str, Any]) -> None:
        path = self.report_log_path
        assert path is not None
        path.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps(payload, separators=(",", ":"), sort_keys=True)
        with path.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")

    @staticmethod
    def _default_post(url: str, body: bytes, timeout: float) -> tuple[int, str]:
        req = urllib.request.Request(
            url,
            data=body,
            method="POST",
            headers={
                "Content-Type": "application/json",
                "User-Agent": f"listener-5.5/{__version__}",
            },
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return int(resp.status), resp.read().decode("utf-8", errors="replace")

    def alive(self) -> bool:
        return bool(self._thread and self._thread.is_alive())

    def ensure_running(self) -> bool:
        """Restart the loop if it should be running and is not. True if healed."""
        if self._wanted and not self.alive():
            self.restarts += 1
            log.warning("reporter thread was dead; restarting (restart #%s)", self.restarts)
            self.start()
            return True
        return False

    def start(self) -> None:
        self._wanted = True
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._loop, name="listener55-reporter", daemon=True
        )
        self._thread.start()

    def stop(self, timeout: float = 2.0) -> None:
        self._wanted = False
        self._stop.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=timeout)

    def _loop(self) -> None:
        # Emit immediately on start so deploy verification is fast.
        try:
            self.emit_once()
        except Exception:  # noqa: BLE001 — never kill the reporter loop
            log.exception("initial self-report crashed")
        while not self._stop.wait(self.interval_seconds):
            try:
                self.emit_once()
            except Exception:  # noqa: BLE001
                log.exception("self-report loop iteration crashed")

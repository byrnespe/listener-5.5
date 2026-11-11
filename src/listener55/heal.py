"""Self-healing primitives: circuit breaker, systemd watchdog, playbooks.

Three layers, innermost first:

1.  **In-process** -- a circuit breaker stops a dead report sink from being
    hammered every cycle; the supervisor (``brain.py``) restarts worker
    threads that died.
2.  **Process** -- ``sd_notify`` lets systemd restart the whole service when
    the supervisor itself stops ticking. Something has to watch the watcher,
    and it should not live in the same address space.
3.  **Fleet** -- playbooks: operator-defined actions the service may take when
    a *peer* is unhealthy.

Playbooks are where this could become dangerous, so the boundary is explicit:
**an operator writes them in a file; nothing arriving over the network can
define one.** An agent can ask for a named playbook to run. It cannot supply a
command line. Commands run as an argv list with no shell, and the only values
substituted into them are ids that pass a strict character whitelist --
service ids come from self-reports, which anyone who can reach the port can
send.
"""

from __future__ import annotations

import fnmatch
import json
import logging
import os
import random
import re
import socket
import subprocess
import threading
import time
import urllib.error
import urllib.request
from collections import deque
from pathlib import Path
from typing import Any, Callable

from listener55.schema import utc_now_iso

log = logging.getLogger("listener55.heal")

SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")
PLAYBOOK_NAME = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")
MAX_TIMEOUT = 300.0
OUTPUT_TAIL = 600


# --------------------------------------------------------------- breaker


class CircuitBreaker:
    """Closed -> open after N straight failures -> half-open probe -> closed.

    While open, calls are skipped until a backoff expires. The backoff doubles
    per consecutive trip, is capped, and carries jitter so a fleet of
    reporters does not retry a recovering sink in lockstep.
    """

    def __init__(
        self,
        *,
        failure_threshold: int = 3,
        base_delay: float = 5.0,
        max_delay: float = 600.0,
        clock: Callable[[], float] = time.monotonic,
        rng: Callable[[], float] = random.random,
    ) -> None:
        self.failure_threshold = failure_threshold
        self.base_delay = base_delay
        self.max_delay = max_delay
        self._clock = clock
        self._rng = rng
        self._lock = threading.Lock()
        self.failures = 0
        self.trips = 0
        self._consecutive_trips = 0
        self._open_until: float | None = None

    @property
    def state(self) -> str:
        with self._lock:
            if self._open_until is None:
                return "closed"
            return "open" if self._clock() < self._open_until else "half_open"

    def allow(self) -> bool:
        return self.state != "open"

    def record_success(self) -> None:
        with self._lock:
            self.failures = 0
            self._consecutive_trips = 0
            self._open_until = None

    def record_failure(self) -> None:
        with self._lock:
            self.failures += 1
            was_probing = self._open_until is not None
            if was_probing or self.failures >= self.failure_threshold:
                self.trips += 1
                self._consecutive_trips += 1
                delay = min(self.max_delay, self.base_delay * (2 ** (self._consecutive_trips - 1)))
                delay *= 0.75 + 0.5 * self._rng()
                self._open_until = self._clock() + delay

    def describe(self) -> dict[str, Any]:
        state = self.state
        with self._lock:
            retry_in = None
            if self._open_until is not None:
                retry_in = round(max(0.0, self._open_until - self._clock()), 3)
            return {
                "state": state,
                "consecutive_failures": self.failures,
                "trips": self.trips,
                "retry_in_seconds": retry_in,
            }


# -------------------------------------------------------------- sd_notify


def sd_notify(message: str) -> bool:
    """Send one datagram to systemd's notify socket. No-op outside systemd."""
    addr = os.environ.get("NOTIFY_SOCKET")
    if not addr:
        return False
    if addr.startswith("@"):
        addr = "\0" + addr[1:]  # abstract namespace
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as sock:
            sock.sendto(message.encode("utf-8"), addr)
        return True
    except OSError as exc:
        log.debug("sd_notify failed: %s", exc)
        return False


# -------------------------------------------------------------- playbooks


def _default_runner(argv: list[str], timeout: float) -> tuple[bool, str]:
    try:
        proc = subprocess.run(  # noqa: S603 -- argv list, no shell, operator-defined
            argv,
            capture_output=True,
            text=True,
            timeout=timeout,
            stdin=subprocess.DEVNULL,
        )
    except subprocess.TimeoutExpired:
        return False, f"timed out after {timeout:g}s"
    except OSError as exc:
        return False, f"could not run: {exc}"
    tail = ((proc.stdout or "") + (proc.stderr or "")).strip()[-OUTPUT_TAIL:]
    return proc.returncode == 0, f"exit {proc.returncode}" + (f": {tail}" if tail else "")


def _default_poster(url: str, method: str, headers: dict[str, str], body: bytes, timeout: float) -> tuple[bool, str]:
    req = urllib.request.Request(url, data=body, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310 -- scheme checked at load
            return 200 <= resp.status < 300, f"http {resp.status}"
    except urllib.error.HTTPError as exc:
        return False, f"http {exc.code}"
    except (urllib.error.URLError, TimeoutError, OSError, ValueError) as exc:
        return False, f"http error: {exc}"


def _validate_playbook(raw: Any) -> tuple[dict[str, Any] | None, str | None]:
    if not isinstance(raw, dict):
        return None, "playbook must be an object"
    name = raw.get("name")
    if not isinstance(name, str) or not PLAYBOOK_NAME.match(name):
        return None, f"bad playbook name {name!r} (lowercase letters, digits, - and _)"
    match = raw.get("match") or {}
    if not isinstance(match, dict):
        return None, f"{name}: match must be an object"
    service = match.get("service", "*")
    kinds = match.get("kinds", ["stale"])
    if not isinstance(service, str) or not service:
        return None, f"{name}: match.service must be a non-empty string"
    if not isinstance(kinds, list) or not kinds or not all(isinstance(k, str) for k in kinds):
        return None, f"{name}: match.kinds must be a non-empty list of strings"
    action = raw.get("action")
    if not isinstance(action, dict):
        return None, f"{name}: action must be an object"
    atype = action.get("type")
    timeout = action.get("timeout", 30)
    if not isinstance(timeout, (int, float)) or isinstance(timeout, bool) or not (0 < timeout <= MAX_TIMEOUT):
        return None, f"{name}: action.timeout must be in (0, {MAX_TIMEOUT:g}]"
    if atype == "command":
        argv = action.get("argv")
        if not isinstance(argv, list) or not argv or not all(isinstance(a, str) and a for a in argv):
            return None, f"{name}: command action needs argv as a non-empty list of strings"
        clean_action: dict[str, Any] = {"type": "command", "argv": list(argv), "timeout": float(timeout)}
    elif atype == "webhook":
        url = action.get("url")
        if not isinstance(url, str) or not url.lower().startswith(("http://", "https://")):
            return None, f"{name}: webhook action needs an http(s) url"
        headers = action.get("headers") or {}
        if not isinstance(headers, dict) or not all(
            isinstance(k, str) and isinstance(v, str) for k, v in headers.items()
        ):
            return None, f"{name}: webhook headers must map strings to strings"
        method = str(action.get("method", "POST")).upper()
        if method not in ("POST", "PUT"):
            return None, f"{name}: webhook method must be POST or PUT"
        clean_action = {
            "type": "webhook",
            "url": url,
            "method": method,
            "headers": dict(headers),
            "timeout": float(timeout),
        }
    else:
        return None, f"{name}: unknown action type {atype!r} (command or webhook)"
    cooldown = raw.get("cooldown_seconds", 300)
    attempts = raw.get("max_attempts", 2)
    if not isinstance(cooldown, (int, float)) or isinstance(cooldown, bool) or cooldown < 0:
        return None, f"{name}: cooldown_seconds must be >= 0"
    if not isinstance(attempts, int) or isinstance(attempts, bool) or attempts < 1:
        return None, f"{name}: max_attempts must be an integer >= 1"
    return (
        {
            "name": name,
            "description": str(raw.get("description", ""))[:300],
            "match": {"service": service, "kinds": list(kinds)},
            "action": clean_action,
            "cooldown_seconds": float(cooldown),
            "max_attempts": attempts,
            "verify_seconds": float(raw.get("verify_seconds", 120)),
            "allow_unregistered": bool(raw.get("allow_unregistered", False)),
            "auto": bool(raw.get("auto", True)),
        },
        None,
    )


class Playbooks:
    """Operator-defined remediation actions, ranked by what has worked."""

    def __init__(
        self,
        path: str | None,
        stats: dict[str, Any],
        touch: Callable[[], None],
        *,
        dry_run: bool = False,
        max_per_hour: int = 6,
        runner: Callable[[list[str], float], tuple[bool, str]] | None = None,
        poster: Callable[[str, str, dict[str, str], bytes, float], tuple[bool, str]] | None = None,
        wall: Callable[[], float] = time.time,
    ) -> None:
        self.path = Path(path).expanduser() if path else None
        self._stats = stats
        self._touch = touch
        self.dry_run = dry_run
        self.max_per_hour = max_per_hour
        self._runner = runner or _default_runner
        self._poster = poster or _default_poster
        self._wall = wall
        self._lock = threading.Lock()
        self._books: dict[str, dict[str, Any]] = {}
        self._order: list[str] = []
        self._mtime: float | None = None
        # Cooldowns and the hourly budget are persisted with the outcomes: a
        # restart must not hand out a fresh allowance.
        runtime = stats.get("_runtime")
        if not isinstance(runtime, dict):
            runtime = stats["_runtime"] = {}
        if not isinstance(runtime.get("last_run"), dict):
            runtime["last_run"] = {}
        if not isinstance(runtime.get("recent"), list):
            runtime["recent"] = []
        self._last_run: dict[str, float] = runtime["last_run"]
        self._recent_runs: list[float] = runtime["recent"]
        self.load_errors: list[str] = []
        self.reload()

    # ----------------------------------------------------------------- load

    def reload(self) -> bool:
        """(Re)read the file. A bad file never takes the service down, and a
        bad entry never takes the good ones with it."""
        books: dict[str, dict[str, Any]] = {}
        order: list[str] = []
        errors: list[str] = []
        mtime: float | None = None
        if self.path is not None:
            try:
                mtime = self.path.stat().st_mtime
                doc = json.loads(self.path.read_text(encoding="utf-8"))
                entries = doc.get("playbooks") if isinstance(doc, dict) else None
                if not isinstance(entries, list):
                    raise ValueError('expected {"playbooks": [...]}')
                for raw in entries:
                    book, err = _validate_playbook(raw)
                    if book is None:
                        errors.append(err or "invalid playbook")
                    elif book["name"] in books:
                        errors.append(f"duplicate playbook name {book['name']!r}")
                    else:
                        books[book["name"]] = book
                        order.append(book["name"])
            except (OSError, ValueError) as exc:
                errors.append(f"{type(exc).__name__}: {exc}")
        with self._lock:
            self._books, self._order, self._mtime, self.load_errors = books, order, mtime, errors
        for err in errors:
            log.warning("playbooks: %s", err)
        return not errors

    def reload_if_changed(self) -> bool:
        if self.path is None:
            return False
        try:
            mtime: float | None = self.path.stat().st_mtime
        except OSError:
            mtime = None
        if mtime != self._mtime:
            self.reload()
            return True
        return False

    # ---------------------------------------------------------------- query

    def _score(self, name: str, kind: str) -> tuple[float, int, int]:
        st = self._stats.get(f"{name}|{kind}") or {}
        ok, fail = int(st.get("ok", 0)), int(st.get("fail", 0))
        # Laplace-smoothed success rate: an untried playbook scores 0.5, so it
        # outranks one that has mostly failed and trails one that mostly works.
        return (ok + 1) / (ok + fail + 2), ok, fail

    def candidates(self, incident: dict[str, Any], *, registered: bool) -> list[dict[str, Any]]:
        """Playbooks that apply to this incident, best first."""
        now = self._wall()
        out: list[dict[str, Any]] = []
        with self._lock:
            for idx, name in enumerate(self._order):
                book = self._books[name]
                if incident["kind"] not in book["match"]["kinds"]:
                    continue
                if not fnmatch.fnmatchcase(incident["service"], book["match"]["service"]):
                    continue
                score, ok, fail = self._score(name, incident["kind"])
                # In global dry-run mode a logged dry run is the attempt;
                # otherwise the same "would run" line repeats every tick.
                used = sum(
                    1
                    for a in incident.get("actions", [])
                    if a.get("playbook") == name and (self.dry_run or not a.get("dry_run"))
                )
                last = self._last_run.get(f"{name}|{incident['service']}")
                cooldown_left = max(0.0, book["cooldown_seconds"] - (now - last)) if last else 0.0
                blocked: str | None = None
                if not SAFE_ID.match(incident["service"]):
                    blocked = "service id is not a plain identifier"
                elif not self.dry_run and self._runs_last_hour() >= self.max_per_hour:
                    blocked = f"action budget exhausted ({self.max_per_hour}/hour)"
                elif not registered and not book["allow_unregistered"]:
                    blocked = "service is not in the registry and playbook does not allow_unregistered"
                elif used >= book["max_attempts"]:
                    blocked = "max_attempts reached for this incident"
                elif cooldown_left > 0:
                    blocked = f"cooling down for {cooldown_left:.0f}s"
                out.append(
                    {
                        "playbook": name,
                        "description": book["description"],
                        "action_type": book["action"]["type"],
                        "success_rate": round(score, 3),
                        "succeeded": ok,
                        "failed": fail,
                        "attempts_this_incident": used,
                        "auto": book["auto"],
                        "runnable": blocked is None,
                        "blocked_reason": blocked,
                        "_order": idx,
                    }
                )
        out.sort(key=lambda c: (-c["success_rate"], c["_order"]))
        for c in out:
            del c["_order"]
        return out

    def get(self, name: str) -> dict[str, Any] | None:
        with self._lock:
            return self._books.get(name)

    def describe(self) -> dict[str, Any]:
        with self._lock:
            books = [
                {
                    "name": b["name"],
                    "description": b["description"],
                    "match": b["match"],
                    "action_type": b["action"]["type"],
                    "cooldown_seconds": b["cooldown_seconds"],
                    "max_attempts": b["max_attempts"],
                    "auto": b["auto"],
                    "outcomes": {
                        k.split("|", 1)[1]: v
                        for k, v in self._stats.items()
                        if k != "_runtime" and k.startswith(b["name"] + "|")
                    },
                }
                for b in (self._books[n] for n in self._order)
            ]
            return {
                "path": str(self.path) if self.path else None,
                "dry_run": self.dry_run,
                "max_actions_per_hour": self.max_per_hour,
                "actions_last_hour": self._runs_last_hour(),
                "load_errors": list(self.load_errors),
                "playbooks": books,
            }

    def _runs_last_hour(self) -> int:
        cutoff = self._wall() - 3600
        while self._recent_runs and self._recent_runs[0] < cutoff:
            self._recent_runs.pop(0)
        return len(self._recent_runs)

    # ------------------------------------------------------------------ run

    def run(
        self,
        name: str,
        incident: dict[str, Any],
        *,
        actor: str,
        registered: bool,
        dry_run: bool | None = None,
    ) -> dict[str, Any]:
        """Run one playbook against one incident. Always returns a record;
        refusals are records with ``ok: False`` and ``refused: True``."""
        dry = self.dry_run if dry_run is None else (dry_run or self.dry_run)
        record: dict[str, Any] = {
            "at": utc_now_iso(),
            "playbook": name,
            "actor": actor,
            "dry_run": dry,
            "ok": False,
        }

        def refuse(why: str) -> dict[str, Any]:
            record.update(refused=True, detail=why)
            return record

        book = self.get(name)
        if book is None:
            return refuse("no such playbook")
        service, kind = incident["service"], incident["kind"]
        if incident.get("status") != "open":
            return refuse("incident is not open")
        if kind not in book["match"]["kinds"] or not fnmatch.fnmatchcase(service, book["match"]["service"]):
            return refuse("playbook does not match this incident")
        if not SAFE_ID.match(service):
            # The id came off the wire. It is about to be put in an argv or a
            # URL-bound body, so it has to look like an id.
            return refuse("service id contains characters not allowed in a playbook target")
        if not registered and not book["allow_unregistered"]:
            return refuse("service is not in the registry and playbook does not allow_unregistered")
        action = book["action"]
        record["action_type"] = action["type"]
        subs = {"service": service, "incident_id": incident["id"], "kind": kind}
        if action["type"] == "command":
            try:
                argv = [a.format(**subs) for a in action["argv"]]
            except (KeyError, IndexError, ValueError) as exc:
                return refuse(f"bad placeholder in argv: {exc}")
            record["argv"] = argv

        if dry:
            record.update(ok=True, detail="dry run: nothing executed")
            return record

        with self._lock:
            used = sum(
                1
                for a in incident.get("actions", [])
                if a.get("playbook") == name and not a.get("dry_run") and not a.get("refused")
            )
            if used >= book["max_attempts"]:
                return refuse("max_attempts reached for this incident")
            now = self._wall()
            last = self._last_run.get(f"{name}|{service}")
            if last is not None and now - last < book["cooldown_seconds"]:
                return refuse(f"cooling down for {book['cooldown_seconds'] - (now - last):.0f}s")
            if self._runs_last_hour() >= self.max_per_hour:
                return refuse(f"action budget exhausted ({self.max_per_hour}/hour)")
            self._last_run[f"{name}|{service}"] = now
            self._recent_runs.append(now)
            for k in [k for k, t in self._last_run.items() if now - t > 86400]:
                del self._last_run[k]
        self._touch()

        started = time.monotonic()
        try:
            if action["type"] == "command":
                ok, detail = self._runner(record["argv"], action["timeout"])
            else:
                body = json.dumps(
                    {
                        "source": "listener-5.5",
                        "playbook": name,
                        "incident": {
                            k: incident.get(k)
                            for k in ("id", "service", "kind", "severity", "summary", "opened_at", "occurrence")
                        },
                    }
                ).encode("utf-8")
                headers = {"Content-Type": "application/json", **action["headers"]}
                ok, detail = self._poster(action["url"], action["method"], headers, body, action["timeout"])
        except Exception as exc:  # noqa: BLE001 -- a playbook must never crash the caller
            ok, detail = False, f"{type(exc).__name__}: {exc}"
        record.update(ok=ok, detail=detail, elapsed_seconds=round(time.monotonic() - started, 3))
        log.info("playbook %s on %s (%s) by %s: ok=%s %s", name, service, kind, actor, ok, detail)
        return record

    def record_outcome(self, name: str, kind: str, cleared: bool) -> None:
        """Did the incident actually clear after this playbook ran? This, not
        the action's exit code, is what ranking learns from."""
        with self._lock:
            st = self._stats.setdefault(f"{name}|{kind}", {"ok": 0, "fail": 0})
            st["ok" if cleared else "fail"] += 1
        self._touch()

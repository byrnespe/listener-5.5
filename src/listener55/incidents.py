"""Incident memory: what went wrong, how long, what was tried, what worked.

``/fleet`` is a snapshot -- it says what is wrong *now* and forgets the moment
it stops being wrong. This module is the part that remembers. Each problem
becomes an incident with a lifecycle; each incident carries a fingerprint so
the fifth occurrence of the same failure is recognised as the fifth, with the
notes and outcomes of the previous four attached.

That recurrence memory is what later stages lean on: remediation ranks
playbooks by how often they actually cleared this kind of incident, and an
agent asking for a diagnosis gets the history rather than a blank page.
"""

from __future__ import annotations

import hashlib
import re
import threading
import time
from typing import Any, Callable

from listener55.schema import utc_now_iso

MAX_HISTORY = 500
MAX_NOTES = 20
MAX_NOTE_CHARS = 2000
MAX_ACTIONS = 50
#: Fingerprints remembered. Error text comes off the wire, so a peer that
#: varies it could otherwise mint fingerprints without end.
MAX_MEMORY = 500
MAX_SUMMARY_CHARS = 500

_VOLATILE = re.compile(r"0x[0-9a-fA-F]+|\d+")


def fingerprint(service: str, kind: str, error_text: str | None = None) -> str:
    """Stable id for "the same failure". Numbers are masked so a port, pid or
    timestamp in an error message does not make every occurrence unique."""
    norm = _VOLATILE.sub("N", (error_text or "").strip().lower())[:200]
    return hashlib.sha1(f"{kind}|{service}|{norm}".encode("utf-8")).hexdigest()[:12]


def _bounded(evidence: dict[str, Any]) -> dict[str, Any]:
    """Evidence quotes what services said about themselves; keep it short."""
    return {
        k: (v[:MAX_SUMMARY_CHARS] if isinstance(v, str) else v) for k, v in evidence.items()
    }


class IncidentLog:
    def __init__(
        self,
        section: dict[str, Any],
        touch: Callable[[], None],
        *,
        wall: Callable[[], float] = time.time,
    ) -> None:
        self._s = section
        self._touch = touch
        self._wall = wall
        self._lock = threading.RLock()
        self._s.setdefault("seq", 0)
        if not isinstance(self._s.get("open"), dict):
            self._s["open"] = {}
        if not isinstance(self._s.get("history"), list):
            self._s["history"] = []
        if not isinstance(self._s.get("memory"), dict):
            self._s["memory"] = {}

    # ------------------------------------------------------------ lifecycle

    def get_open(self, service: str, kind: str) -> dict[str, Any] | None:
        with self._lock:
            for inc in self._s["open"].values():
                if inc["service"] == service and inc["kind"] == kind:
                    return inc
        return None

    def open(
        self,
        service: str,
        kind: str,
        *,
        severity: str,
        summary: str,
        evidence: dict[str, Any] | None = None,
        error_text: str | None = None,
    ) -> tuple[dict[str, Any], bool]:
        """Open an incident, or return the already-open one. (incident, created)"""
        with self._lock:
            existing = self.get_open(service, kind)
            if existing is not None:
                if evidence:
                    existing["evidence"].update(_bounded(evidence))
                    existing["updated_at"] = utc_now_iso()
                return existing, False
            self._s["seq"] += 1
            fp = fingerprint(service, kind, error_text)
            mem = self._s["memory"].setdefault(
                fp, {"service": service, "kind": kind, "count": 0, "durations": [], "notes": []}
            )
            mem["count"] += 1
            mem["last_seen"] = utc_now_iso()
            if len(self._s["memory"]) > MAX_MEMORY:
                live = {i["fingerprint"] for i in self._s["open"].values()} | {fp}
                # Forget the failures seen least, oldest first; never one that
                # is open right now.
                victims = sorted(
                    (k for k in self._s["memory"] if k not in live),
                    key=lambda k: (self._s["memory"][k]["count"], self._s["memory"][k].get("last_seen", "")),
                )
                for k in victims[: len(self._s["memory"]) - MAX_MEMORY]:
                    del self._s["memory"][k]
            inc = {
                "id": f"inc-{self._s['seq']:06d}",
                "service": service,
                "kind": kind,
                "severity": severity,
                "status": "open",
                "fingerprint": fp,
                "occurrence": mem["count"],
                "summary": summary[:MAX_SUMMARY_CHARS],
                "evidence": _bounded(evidence or {}),
                "opened_at": utc_now_iso(),
                "opened_epoch": self._wall(),
                "updated_at": utc_now_iso(),
                "resolved_at": None,
                "duration_seconds": None,
                "actions": [],
                "notes": [],
            }
            self._s["open"][inc["id"]] = inc
        self._touch()
        return inc, True

    def resolve(self, service: str, kind: str, reason: str = "condition cleared") -> dict[str, Any] | None:
        with self._lock:
            inc = self.get_open(service, kind)
            if inc is None:
                return None
            del self._s["open"][inc["id"]]
            inc["status"] = "resolved"
            inc["resolved_at"] = utc_now_iso()
            inc["duration_seconds"] = round(max(0.0, self._wall() - inc["opened_epoch"]), 3)
            inc["resolution"] = reason
            self._s["history"].append(inc)
            del self._s["history"][:-MAX_HISTORY]
            mem = self._s["memory"].get(inc["fingerprint"])
            if mem is not None:
                mem["durations"] = (mem["durations"] + [inc["duration_seconds"]])[-20:]
        self._touch()
        return inc

    # --------------------------------------------------------------- update

    def find(self, incident_id: str) -> dict[str, Any] | None:
        with self._lock:
            inc = self._s["open"].get(incident_id)
            if inc is not None:
                return inc
            for old in reversed(self._s["history"]):
                if old["id"] == incident_id:
                    return old
        return None

    def add_note(self, incident_id: str, note: str, actor: str) -> dict[str, Any] | None:
        """Attach a note, and remember it against the fingerprint so the next
        occurrence of the same failure starts with it."""
        note = note.strip()[:MAX_NOTE_CHARS]
        if not note:
            return None
        with self._lock:
            inc = self.find(incident_id)
            if inc is None:
                return None
            rec = {"at": utc_now_iso(), "actor": actor, "note": note}
            inc["notes"] = (inc["notes"] + [rec])[-MAX_NOTES:]
            mem = self._s["memory"].get(inc["fingerprint"])
            if mem is not None:
                mem["notes"] = (mem["notes"] + [dict(rec, incident=inc["id"])])[-MAX_NOTES:]
        self._touch()
        return inc

    def add_action(self, incident_id: str, record: dict[str, Any]) -> None:
        with self._lock:
            inc = self.find(incident_id)
            if inc is None:
                return
            inc["actions"] = (inc["actions"] + [record])[-MAX_ACTIONS:]
            inc["updated_at"] = utc_now_iso()
        self._touch()

    def set_field(self, incident_id: str, key: str, value: Any) -> None:
        with self._lock:
            inc = self.find(incident_id)
            if inc is not None:
                inc[key] = value
        self._touch()

    # ---------------------------------------------------------------- query

    def open_incidents(self) -> list[dict[str, Any]]:
        with self._lock:
            return sorted(self._s["open"].values(), key=lambda i: i["id"])

    def list(
        self, status: str = "open", service: str | None = None, limit: int = 50
    ) -> list[dict[str, Any]]:
        with self._lock:
            items: list[dict[str, Any]] = []
            if status in ("resolved", "all"):
                items.extend(self._s["history"])
            if status in ("open", "all"):
                items.extend(self.open_incidents())
        if service:
            items = [i for i in items if i["service"] == service]
        items.sort(key=lambda i: i["id"], reverse=True)
        return items[: max(1, limit)]

    def memory_for(self, fp: str) -> dict[str, Any] | None:
        with self._lock:
            mem = self._s["memory"].get(fp)
            if mem is None:
                return None
            durations = sorted(mem.get("durations", []))
            return {
                "fingerprint": fp,
                "times_seen": mem["count"],
                "last_seen": mem.get("last_seen"),
                "median_duration_seconds": durations[len(durations) // 2] if durations else None,
                "notes": list(mem.get("notes", [])),
            }

    def stats(self) -> dict[str, Any]:
        with self._lock:
            history = list(self._s["history"])
            open_n = len(self._s["open"])
            memory = self._s["memory"]
            recurring = sorted(
                (
                    {"fingerprint": fp, "service": m["service"], "kind": m["kind"], "times_seen": m["count"]}
                    for fp, m in memory.items()
                    if m["count"] > 1
                ),
                key=lambda r: r["times_seen"],
                reverse=True,
            )[:10]
        by_kind: dict[str, int] = {}
        per_service: dict[str, list[float]] = {}
        for inc in history:
            by_kind[inc["kind"]] = by_kind.get(inc["kind"], 0) + 1
            if inc.get("duration_seconds") is not None:
                per_service.setdefault(inc["service"], []).append(inc["duration_seconds"])
        mttr = {
            svc: round(sum(d) / len(d), 3) for svc, d in sorted(per_service.items()) if d
        }
        return {
            "open": open_n,
            "resolved_remembered": len(history),
            "resolved_by_kind": by_kind,
            "mean_time_to_recover_seconds": mttr,
            "most_recurring": recurring,
        }

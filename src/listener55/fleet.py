"""Fleet roster: aggregate org self-reports and join them against the registry.

listener-5.5 already had everything needed to be the org's push-health bus --
an ingest endpoint, a self-report schema, and a stdlib validator -- and nothing
used it. This module adds the aggregation half.

Two health paths exist in the org and they fail in different directions:

*   **Pull** -- the AAIS `liveness` container probes the docker stack over the
    compose bridge. It catches "the process is up but its dependency is not",
    and cannot see anything outside that network.
*   **Push** (this module) -- services POST the self-report contract here.
    It catches "the service never started at all", which no pull probe of an
    unknown service can, and it reaches components the bridge cannot see.

The join against ``contracts/service-registry.json`` is the point. Without a
roster, a service that has never reported is simply absent, which is
indistinguishable from a service that does not exist. With one, it shows up as
``unknown`` -- and *down* versus *never heard from* is the distinction an
operator actually needs.

Stdlib only, matching the rest of the runtime.
"""

from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path
from typing import Any

from listener55.schema import utc_now_iso, validate_payload

#: Default seconds after which a report is considered stale. A service pushing
#: on the contract's default 30s interval gets three missed pushes of slack
#: before it is called out, so one slow cycle is not an incident.
DEFAULT_STALE_AFTER = 90.0

#: Where to look for the org registry when LISTENER55_REGISTRY is unset.
#: Consumers of the registry read it at runtime and degrade gracefully -- this
#: service must still run standalone on a laptop with no org checkout.
REGISTRY_SEARCH_PATHS = (
    "./contracts/service-registry.json",
    "../AAIS-business-cloud/contracts/service-registry.json",
    "~/projects/AAIS-business-cloud/contracts/service-registry.json",
    "/opt/aais-business-cloud/contracts/service-registry.json",
)

#: The org-wide self-report schema. Canonical copy lives in
#: AAIS-business-cloud/contracts/self-report.schema.json; embedded here so the
#: bus can validate inbound reports with no org checkout and no dependencies.
#: Deliberately looser than listener-5.5's own schema/self_report.schema.json:
#: `service` is not pinned and `counts` is open, so any service can report.
ORG_SELF_REPORT_SCHEMA: dict[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": "https://asburyaisolutions.com/schemas/org/self-report.schema.json",
    "title": "AsburyOrgSelfReport",
    "type": "object",
    "required": [
        "schema_version",
        "service",
        "instance_id",
        "status",
        "uptime_seconds",
        "started_at",
        "reported_at",
    ],
    "properties": {
        "schema_version": {"type": "string", "const": "1.0"},
        "service": {"type": "string", "minLength": 1},
        "registry_id": {"type": "string", "minLength": 1},
        "instance_id": {"type": "string", "minLength": 1},
        "status": {
            "type": "string",
            "enum": ["starting", "healthy", "degraded", "stopping"],
        },
        "uptime_seconds": {"type": "number", "minimum": 0},
        "started_at": {"type": "string", "minLength": 1},
        "reported_at": {"type": "string", "minLength": 1},
        "listen": {
            "type": "object",
            "required": ["host", "port"],
            "properties": {
                "host": {"type": "string", "minLength": 1},
                "port": {"type": "integer", "minimum": 1, "maximum": 65535},
            },
        },
        "counts": {"type": "object", "additionalProperties": {"type": "integer", "minimum": 0}},
        "last_error": {"type": ["string", "null"]},
        "degraded_reasons": {"type": "array", "items": {"type": "string"}},
        "meta": {"type": "object"},
    },
}

#: Distinct reporters the bus will track. Keys come off the wire, so without a
#: cap anyone who can reach the port could grow the store without bound.
MAX_SERVICES = 2000
MAX_KEY_CHARS = 128

#: Roster states, worst first. Ordering drives the rollup summary.
STATE_UNKNOWN = "unknown"
STATE_STALE = "stale"
STATE_DEGRADED = "degraded"
STATE_HEALTHY = "healthy"


def resolve_registry_path(explicit: str | None = None) -> Path | None:
    """Find the org registry, or None if this box has no org checkout."""
    candidates = [explicit] if explicit else []
    if not explicit:
        env = os.environ.get("LISTENER55_REGISTRY")
        if env:
            candidates.append(env)
        candidates.extend(REGISTRY_SEARCH_PATHS)
    for raw in candidates:
        if not raw:
            continue
        path = Path(raw).expanduser()
        if path.is_file():
            return path
    return None


class Roster:
    """The expected-services roster, loaded from the org registry.

    A missing or malformed registry is never fatal: the bus still accepts and
    serves reports, it just cannot tell you what is missing. ``load_error``
    carries the reason so ``/fleet`` can say so instead of implying an empty
    org.
    """

    def __init__(self, path: Path | None = None) -> None:
        self.path = path
        self.load_error: str | None = None
        self.services: dict[str, dict[str, Any]] = {}
        self.tailnet: dict[str, Any] = {}
        self._mtime: float | None = None
        self.reloads = 0
        if path is not None:
            self._load(path)
        else:
            self.load_error = "no registry found (set LISTENER55_REGISTRY)"

    def _load(self, path: Path) -> None:
        try:
            self._mtime = path.stat().st_mtime
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            self.load_error = f"{type(exc).__name__}: {exc}"
            return
        if not isinstance(data, dict) or data.get("schema_version") != "1.0":
            version = data.get("schema_version") if isinstance(data, dict) else None
            self.load_error = f"unsupported registry schema_version {version!r}"
            return
        services: dict[str, dict[str, Any]] = {}
        for svc in data.get("services", ()):
            sid = svc.get("id") if isinstance(svc, dict) else None
            if sid:
                services[sid] = svc
        # Swap whole objects rather than mutating in place: readers on other
        # threads see either the old roster or the new one, never half of each.
        self.tailnet = data.get("tailnet", {}) or {}
        self.services = services
        self.load_error = None

    def reload_if_changed(self) -> bool:
        """Pick up an edited registry without a restart.

        A reload that fails keeps the last good roster: a typo in the
        registry must not make every service look unregistered.
        """
        if self.path is None:
            return False
        try:
            mtime = self.path.stat().st_mtime
        except OSError:
            return False
        if mtime == self._mtime:
            return False
        return self.reload()

    def reload(self) -> bool:
        if self.path is None:
            return False
        previous = (self.services, self.tailnet, self.load_error)
        self._load(self.path)
        if self.load_error is not None and previous[0]:
            failed = self.load_error
            self.services, self.tailnet, _ = previous
            self.load_error = None
            self.last_reload_error = failed
            return False
        self.last_reload_error = None
        if self.load_error is not None:
            return False
        self.reloads += 1
        return True

    last_reload_error: str | None = None

    @property
    def loaded(self) -> bool:
        return self.load_error is None and bool(self.services)

    def expected_ids(self) -> list[str]:
        """Services the registry says should be pushing reports.

        Only `live` services with `self_report: true`. A tombstoned or
        unverified row must not produce a permanent `unknown` alarm -- that is
        how a roster turns into noise nobody reads.
        """
        return [
            sid
            for sid, svc in self.services.items()
            if svc.get("self_report") and svc.get("status") == "live"
        ]

    def describe(self) -> dict[str, Any]:
        if not self.loaded:
            return {"loaded": False, "reason": self.load_error, "path": str(self.path) if self.path else None}
        out: dict[str, Any] = {
            "loaded": True,
            "path": str(self.path),
            "services": len(self.services),
            "expected_reporters": len(self.expected_ids()),
        }
        if self.last_reload_error:
            # Serving the last good roster; say so rather than hide it.
            out["reload_error"] = self.last_reload_error
        return out


class FleetStore:
    """Latest self-report per service, joined against the roster."""

    def __init__(
        self,
        roster: Roster | None = None,
        stale_after: float = DEFAULT_STALE_AFTER,
    ) -> None:
        self.roster = roster if roster is not None else Roster(resolve_registry_path())
        self.stale_after = float(stale_after)
        self._lock = threading.Lock()
        # key -> {"report": dict, "received_mono": float, "received_at": str, "count": int}
        self._reports: dict[str, dict[str, Any]] = {}
        #: Optional learned baselines (``learn.Baselines``). When set, each
        #: service is judged against its own cadence instead of ``stale_after``.
        self.advisor: Any = None
        #: Called with (key, signals) when a report reveals a restart, a crash
        #: loop or an error spike.
        self.on_signals: Any = None
        self.max_services = MAX_SERVICES

    # ------------------------------------------------------------- ingest

    @staticmethod
    def key_for(report: dict[str, Any]) -> str:
        """The join key: registry_id when present, else the service name.

        registry_id is the contract's machine key. Falling back to `service`
        keeps reports from a service that predates the field, at the cost of
        not being matchable against the roster.
        """
        return str(report.get("registry_id") or report.get("service") or "").strip()

    def ingest(self, report: Any, *, observe: bool = True) -> tuple[bool, list[str]]:
        """Validate and store one self-report.

        Returns (accepted, errors). A rejected report is not stored: a bus that
        accepts malformed health is worse than one that accepts none.

        ``observe=False`` stores the report without feeding the learned
        baselines; the bus uses it for its own read-time self-ingest, whose
        timing says nothing about a reporting cadence.
        """
        if not isinstance(report, dict):
            return False, ["payload must be a JSON object"]
        errors = validate_payload(report, ORG_SELF_REPORT_SCHEMA)
        if errors:
            return False, errors
        key = self.key_for(report)
        if not key:
            return False, ["report has neither registry_id nor service"]
        if len(key) > MAX_KEY_CHARS:
            return False, [f"service key longer than {MAX_KEY_CHARS} characters"]
        now = time.monotonic()
        with self._lock:
            previous = self._reports.get(key)
            if previous is None and len(self._reports) >= self.max_services:
                return False, [f"fleet store is full ({self.max_services} services)"]
            self._reports[key] = {
                "report": report,
                "received_mono": now,
                "received_at": utc_now_iso(),
                "count": (previous["count"] + 1) if previous else 1,
            }
        if observe and self.advisor is not None:
            signals = self.advisor.observe(key, report, now)
            if signals and self.on_signals is not None:
                self.on_signals(key, signals)
        return True, []

    def forget(self, key: str) -> bool:
        with self._lock:
            return self._reports.pop(key, None) is not None

    def prune(self, older_than_seconds: float) -> list[str]:
        """Drop unregistered reporters that have been silent a long time.

        Registered services are never pruned: their silence is the signal.
        """
        now = time.monotonic()
        with self._lock:
            gone = [
                k
                for k, e in self._reports.items()
                if k not in self.roster.services and now - e["received_mono"] > older_than_seconds
            ]
            for k in gone:
                del self._reports[k]
        return gone

    def threshold_for(self, key: str) -> tuple[float, str]:
        """(stale-after seconds, 'learned' | 'configured') for one service."""
        if self.advisor is not None:
            return self.advisor.threshold(key, self.stale_after)
        return self.stale_after, "configured"

    # -------------------------------------------------------------- query

    def _state_for(
        self, entry: dict[str, Any] | None, now: float, stale_after: float | None = None
    ) -> tuple[str, float | None]:
        if entry is None:
            return STATE_UNKNOWN, None
        age = now - entry["received_mono"]
        if age > (self.stale_after if stale_after is None else stale_after):
            return STATE_STALE, age
        status = entry["report"].get("status")
        if status in ("degraded", "stopping"):
            return STATE_DEGRADED, age
        return STATE_HEALTHY, age

    def _row(
        self,
        key: str,
        entry: dict[str, Any] | None,
        now: float,
        learned: frozenset[str] = frozenset(),
    ) -> dict[str, Any]:
        stale_after, source = self.threshold_for(key)
        state, age = self._state_for(entry, now, stale_after)
        svc = self.roster.services.get(key)
        row: dict[str, Any] = {
            "id": key,
            "state": state,
            # registered=False means something is pushing health that the
            # registry has never heard of -- worth surfacing, not hiding.
            "registered": svc is not None,
            "expected": bool(svc and svc.get("self_report") and svc.get("status") == "live"),
            "stale_after_seconds": round(stale_after, 3),
            "threshold_source": source,
        }
        if key in learned and not row["expected"]:
            # Not promised by the registry, but seen often enough that the
            # bus remembers it and will notice it going quiet.
            row["learned"] = True
        if svc:
            row["name"] = svc.get("name")
            row["cluster"] = svc.get("cluster")
            row["port"] = svc.get("port")
            row["bind"] = svc.get("bind")
            row["repo"] = svc.get("repo")
        if entry is None:
            row["last_report_at"] = None
            row["reports_received"] = 0
            return row
        report = entry["report"]
        row["last_report_at"] = entry["received_at"]
        row["age_seconds"] = round(age, 3) if age is not None else None
        row["reports_received"] = entry["count"]
        row["status"] = report.get("status")
        row["service"] = report.get("service")
        row["instance_id"] = report.get("instance_id")
        row["uptime_seconds"] = report.get("uptime_seconds")
        row["counts"] = report.get("counts")
        if report.get("last_error"):
            row["last_error"] = report["last_error"]
        if report.get("degraded_reasons"):
            row["degraded_reasons"] = report["degraded_reasons"]
        return row

    def rollup(self) -> dict[str, Any]:
        """The full fleet view: everything reporting plus everything expected."""
        now = time.monotonic()
        with self._lock:
            entries = dict(self._reports)
        learned = frozenset(self.advisor.learned_ids()) if self.advisor is not None else frozenset()
        keys = set(entries) | set(self.roster.expected_ids()) | learned
        rows = [self._row(k, entries.get(k), now, learned) for k in sorted(keys)]

        summary = {STATE_HEALTHY: 0, STATE_DEGRADED: 0, STATE_STALE: 0, STATE_UNKNOWN: 0}
        for row in rows:
            summary[row["state"]] = summary.get(row["state"], 0) + 1

        # Only *expected* services can degrade the fleet verdict. An
        # unregistered reporter is informational; an unverified registry row is
        # not a promise that anything is running.
        actionable = [r for r in rows if r["expected"] and r["state"] != STATE_HEALTHY]
        return {
            "status": "ok" if not actionable else "degraded",
            "checked_at": utc_now_iso(),
            "stale_after_seconds": self.stale_after,
            "registry": self.roster.describe(),
            "summary": {**summary, "total": len(rows)},
            "needs_attention": [r["id"] for r in actionable],
            # Remembered-but-unregistered peers that have gone quiet or
            # degraded. Informational: they never change `status`.
            "learned_attention": [
                r["id"] for r in rows if r.get("learned") and r["state"] != STATE_HEALTHY
            ],
            "services": rows,
        }

    def one(self, key: str) -> dict[str, Any] | None:
        """One service's row, or None if it is neither reporting nor expected."""
        now = time.monotonic()
        with self._lock:
            entry = self._reports.get(key)
        learned = frozenset(self.advisor.learned_ids()) if self.advisor is not None else frozenset()
        if entry is None and key not in self.roster.services and key not in learned:
            return None
        return self._row(key, entry, now, learned)

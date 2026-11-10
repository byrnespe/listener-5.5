"""Self-report payload builder + schema validation."""

from __future__ import annotations

import json
import threading
import time
from collections import deque
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from listener55 import __service_name__, __version__

SCHEMA_VERSION = "1.0"

# Embedded canonical schema (kept in sync with schema/self_report.schema.json).
SELF_REPORT_SCHEMA: dict[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": "https://asburyaisolutions.com/schemas/listener55/self-report.schema.json",
    "title": "Listener55SelfReport",
    "type": "object",
    "additionalProperties": False,
    "required": [
        "schema_version",
        "service",
        "instance_id",
        "status",
        "uptime_seconds",
        "started_at",
        "reported_at",
        "listen",
        "counts",
        "last_error",
    ],
    "properties": {
        "schema_version": {"type": "string", "const": "1.0"},
        "service": {"type": "string", "const": "listener-5.5"},
        "instance_id": {"type": "string", "minLength": 1},
        "status": {
            "type": "string",
            "enum": ["starting", "healthy", "degraded", "stopping"],
        },
        "uptime_seconds": {"type": "number", "minimum": 0},
        "started_at": {"type": "string"},
        "reported_at": {"type": "string"},
        "listen": {
            "type": "object",
            "additionalProperties": False,
            "required": ["host", "port"],
            "properties": {
                "host": {"type": "string", "minLength": 1},
                "port": {"type": "integer", "minimum": 1, "maximum": 65535},
            },
        },
        "counts": {
            "type": "object",
            "additionalProperties": False,
            "required": [
                "received",
                "processed_ok",
                "processed_error",
                "self_reports_sent",
                "self_reports_failed",
            ],
            "properties": {
                "received": {"type": "integer", "minimum": 0},
                "processed_ok": {"type": "integer", "minimum": 0},
                "processed_error": {"type": "integer", "minimum": 0},
                "self_reports_sent": {"type": "integer", "minimum": 0},
                "self_reports_failed": {"type": "integer", "minimum": 0},
            },
        },
        "last_error": {"type": ["string", "null"]},
        "meta": {"type": "object"},
    },
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


#: Health is judged over a sliding window rather than by the last event, so one
#: bad payload does not mark the service degraded and one good one does not
#: clear a real problem. Errors also age out on their own: a service that
#: failed an hour ago and has been idle since is healthy, not degraded forever.
HEALTH_WINDOW_SECONDS = 300.0
HEALTH_WINDOW_EVENTS = 50
HEALTH_ERROR_RATIO = 0.2
#: Consecutive failed self-reports before the sink counts as down.
SINK_FAILURE_STREAK = 3


class Metrics:
    """Thread-safe counters for the listener."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._outcomes: deque[tuple[float, bool]] = deque(maxlen=HEALTH_WINDOW_EVENTS)
        self.report_failure_streak = 0
        self.internal_errors = 0
        self.deduplicated = 0
        self._reasons: list[str] = []
        self.received = 0
        self.processed_ok = 0
        self.processed_error = 0
        self.self_reports_sent = 0
        self.self_reports_failed = 0
        self.last_error: str | None = None
        self.status = "starting"
        self.started_at_mono = time.monotonic()
        self.started_at_iso = utc_now_iso()

    def snapshot_counts(self) -> dict[str, int]:
        with self._lock:
            return {
                "received": self.received,
                "processed_ok": self.processed_ok,
                "processed_error": self.processed_error,
                "self_reports_sent": self.self_reports_sent,
                "self_reports_failed": self.self_reports_failed,
            }

    def mark_received(self) -> None:
        with self._lock:
            self.received += 1

    def _reassess_locked(self) -> None:
        """Derive status from the recent window. Caller holds the lock."""
        if self.status == "stopping":
            return
        cutoff = time.monotonic() - HEALTH_WINDOW_SECONDS
        while self._outcomes and self._outcomes[0][0] < cutoff:
            self._outcomes.popleft()
        reasons: list[str] = []
        total = len(self._outcomes)
        errors = sum(1 for _, ok in self._outcomes if not ok)
        if errors and errors / total > HEALTH_ERROR_RATIO:
            reasons.append(
                f"event error rate {errors}/{total} over the last "
                f"{int(HEALTH_WINDOW_SECONDS)}s"
            )
        if self.report_failure_streak >= SINK_FAILURE_STREAK:
            reasons.append(
                f"self-report sink failing ({self.report_failure_streak} consecutive)"
            )
        self._reasons = reasons
        if reasons:
            self.status = "degraded"
        elif self.status == "degraded" or (self.status == "starting" and total):
            self.status = "healthy"
        if not reasons and not errors and self.report_failure_streak == 0:
            self.last_error = None

    def mark_ok(self) -> None:
        with self._lock:
            self.processed_ok += 1
            self._outcomes.append((time.monotonic(), True))
            self._reassess_locked()

    def mark_error(self, err: str) -> None:
        with self._lock:
            self.processed_error += 1
            self.last_error = err[:500]
            self._outcomes.append((time.monotonic(), False))
            self._reassess_locked()

    def mark_report_sent(self) -> None:
        with self._lock:
            self.self_reports_sent += 1
            self.report_failure_streak = 0
            if self.last_error and self.last_error.startswith("self-report:"):
                self.last_error = None
            self._reassess_locked()

    def mark_report_failed(self, err: str) -> None:
        with self._lock:
            self.self_reports_failed += 1
            self.report_failure_streak += 1
            self.last_error = f"self-report: {err[:400]}"
            self._reassess_locked()

    def mark_internal_error(self) -> None:
        with self._lock:
            self.internal_errors += 1

    def mark_deduplicated(self) -> None:
        with self._lock:
            self.deduplicated += 1

    def set_status(self, status: str) -> None:
        with self._lock:
            self.status = status

    def degraded_reasons(self) -> list[str]:
        with self._lock:
            self._reassess_locked()
            return list(self._reasons)

    def uptime_seconds(self) -> float:
        return max(0.0, time.monotonic() - self.started_at_mono)

    def build_payload(
        self,
        *,
        instance_id: str,
        host: str,
        port: int,
        meta: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        with self._lock:
            self._reassess_locked()  # let old errors age out
            payload: dict[str, Any] = {
                "schema_version": SCHEMA_VERSION,
                "service": __service_name__,
                "instance_id": instance_id,
                "status": self.status,
                "uptime_seconds": round(self.uptime_seconds(), 3),
                "started_at": self.started_at_iso,
                "reported_at": utc_now_iso(),
                "listen": {"host": host, "port": port},
                "counts": {
                    "received": self.received,
                    "processed_ok": self.processed_ok,
                    "processed_error": self.processed_error,
                    "self_reports_sent": self.self_reports_sent,
                    "self_reports_failed": self.self_reports_failed,
                },
                "last_error": self.last_error,
            }
        if meta is not None:
            payload["meta"] = meta
        else:
            payload["meta"] = {"version": __version__}
        return payload


def _type_ok(value: Any, declared: Any) -> bool:
    if isinstance(declared, list):
        return any(_type_ok(value, d) for d in declared)
    if declared == "object":
        return isinstance(value, dict)
    if declared == "string":
        return isinstance(value, str)
    if declared == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if declared == "number":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if declared == "null":
        return value is None
    if declared == "boolean":
        return isinstance(value, bool)
    if declared == "array":
        return isinstance(value, list)
    return True


def validate_payload(payload: dict[str, Any], schema: dict[str, Any] | None = None) -> list[str]:
    """Minimal JSON Schema validator covering the self-report schema.

    Prefer jsonschema when installed; fall back to a targeted checker so the
    runtime stays stdlib-only.
    """
    schema = schema or SELF_REPORT_SCHEMA
    try:
        import jsonschema  # type: ignore

        validator = jsonschema.Draft202012Validator(schema)
        return sorted(e.message for e in validator.iter_errors(payload))
    except ImportError:
        return _validate_local(payload, schema)


def _validate_local(payload: Any, schema: dict[str, Any], path: str = "$") -> list[str]:
    errors: list[str] = []
    stype = schema.get("type")
    if stype is not None and not _type_ok(payload, stype):
        errors.append(f"{path}: expected type {stype}, got {type(payload).__name__}")
        return errors

    if "const" in schema and payload != schema["const"]:
        errors.append(f"{path}: expected const {schema['const']!r}")

    if "enum" in schema and payload not in schema["enum"]:
        errors.append(f"{path}: value not in enum {schema['enum']}")

    if isinstance(payload, str) and "minLength" in schema:
        if len(payload) < schema["minLength"]:
            errors.append(f"{path}: string shorter than minLength")

    if isinstance(payload, (int, float)) and not isinstance(payload, bool):
        if "minimum" in schema and payload < schema["minimum"]:
            errors.append(f"{path}: below minimum {schema['minimum']}")
        if "maximum" in schema and payload > schema["maximum"]:
            errors.append(f"{path}: above maximum {schema['maximum']}")

    if isinstance(payload, dict) and schema.get("type") in ("object", None):
        required = schema.get("required", [])
        for key in required:
            if key not in payload:
                errors.append(f"{path}: missing required property {key!r}")
        props = schema.get("properties", {})
        additional = schema.get("additionalProperties", True)
        for key, value in payload.items():
            if key in props:
                errors.extend(_validate_local(value, props[key], f"{path}.{key}"))
            elif additional is False:
                errors.append(f"{path}: additional property not allowed: {key!r}")
            elif isinstance(additional, dict):
                errors.extend(_validate_local(value, additional, f"{path}.{key}"))
    return errors


def assert_valid(payload: dict[str, Any]) -> dict[str, Any]:
    errs = validate_payload(payload)
    if errs:
        raise ValueError("invalid self-report payload: " + "; ".join(errs))
    return payload


def load_schema_from_repo(repo_root: Path | None = None) -> dict[str, Any]:
    if repo_root is None:
        # src/listener55/schema.py -> repo root
        repo_root = Path(__file__).resolve().parents[2]
    path = repo_root / "schema" / "self_report.schema.json"
    with path.open(encoding="utf-8") as fh:
        return json.load(fh)


def schema_copy() -> dict[str, Any]:
    return deepcopy(SELF_REPORT_SCHEMA)

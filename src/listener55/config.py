"""Environment-driven configuration for listener-5.5."""

from __future__ import annotations

import os
import uuid
from dataclasses import dataclass
from pathlib import Path


def _env(name: str, default: str | None = None) -> str | None:
    val = os.environ.get(name)
    if val is None or val == "":
        return default
    return val


def _env_int(name: str, default: int) -> int:
    raw = _env(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from exc


def _env_float(name: str, default: float) -> float:
    raw = _env(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a float, got {raw!r}") from exc


@dataclass(frozen=True)
class Config:
    """Runtime config. All knobs via env vars (see README)."""

    host: str
    port: int
    report_url: str | None
    report_interval_seconds: float
    report_log_path: Path | None
    instance_id: str
    max_body_bytes: int
    process_fail_rate: float  # test hook only; 0.0 in prod
    # Fleet-bus knobs. Defaulted so an existing caller that builds Config
    # positionally keeps working.
    registry_id: str = "listener-55"  # this service's id in the org registry
    registry_path: str | None = None  # org registry path; None = auto-discover
    fleet_stale_after_seconds: float = 90.0

    @classmethod
    def from_env(cls) -> "Config":
        log_raw = _env("LISTENER55_REPORT_LOG")
        report_url = _env("LISTENER55_REPORT_URL")
        if log_raw:
            report_log: Path | None = Path(log_raw).expanduser()
        elif report_url is None:
            # Default sink so the service always self-reports somewhere.
            report_log = Path(
                os.environ.get("LISTENER55_DEFAULT_LOG", "listener55-self-report.jsonl")
            )
        else:
            report_log = None
        instance = _env("LISTENER55_INSTANCE_ID") or str(uuid.uuid4())
        return cls(
            host=_env("LISTENER55_HOST", "127.0.0.1") or "127.0.0.1",
            port=_env_int("LISTENER55_PORT", 8555),
            report_url=report_url,
            report_interval_seconds=_env_float("LISTENER55_REPORT_INTERVAL", 30.0),
            report_log_path=report_log,
            instance_id=instance,
            max_body_bytes=_env_int("LISTENER55_MAX_BODY_BYTES", 1_048_576),
            process_fail_rate=_env_float("LISTENER55_PROCESS_FAIL_RATE", 0.0),
            registry_id=_env("LISTENER55_REGISTRY_ID", "listener-55") or "listener-55",
            registry_path=_env("LISTENER55_REGISTRY"),
            fleet_stale_after_seconds=_env_float(
                "LISTENER55_FLEET_STALE_AFTER", 90.0
            ),
        )

    def validate(self) -> None:
        if not (1 <= self.port <= 65535):
            raise ValueError(f"port out of range: {self.port}")
        if self.report_interval_seconds <= 0:
            raise ValueError("LISTENER55_REPORT_INTERVAL must be > 0")
        if self.max_body_bytes < 1:
            raise ValueError("LISTENER55_MAX_BODY_BYTES must be >= 1")
        if not (0.0 <= self.process_fail_rate <= 1.0):
            raise ValueError("LISTENER55_PROCESS_FAIL_RATE must be in [0,1]")
        if self.report_url is None and self.report_log_path is None:
            raise ValueError("need LISTENER55_REPORT_URL or LISTENER55_REPORT_LOG")
        if self.fleet_stale_after_seconds <= 0:
            raise ValueError("LISTENER55_FLEET_STALE_AFTER must be > 0")
        if not self.registry_id:
            raise ValueError("LISTENER55_REGISTRY_ID must not be empty")

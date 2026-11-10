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


def _env_bool(name: str, default: bool) -> bool:
    raw = _env(name)
    if raw is None:
        return default
    low = raw.strip().lower()
    if low in ("1", "true", "yes", "on"):
        return True
    if low in ("0", "false", "no", "off"):
        return False
    raise ValueError(f"{name} must be a boolean (1/0, true/false), got {raw!r}")


def default_state_dir() -> str | None:
    """Where learned state lives. ``off``/``none`` keeps everything in memory."""
    raw = _env("LISTENER55_STATE_DIR")
    if raw is not None:
        if raw.strip().lower() in ("off", "none", "memory"):
            return None
        return str(Path(raw).expanduser())
    base = _env("XDG_STATE_HOME") or "~/.local/state"
    return str(Path(base).expanduser() / "listener55")


#: Autonomy levels, least to most. The configured level is a ceiling: an agent
#: may lower it at runtime but never raise it past what the operator set.
AUTONOMY_LEVELS = ("observe", "suggest", "act")


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
    # Self-healing / learning / agent knobs (0.2). All defaulted for the same
    # reason; a Config built by hand gets an in-memory, token-less service.
    state_dir: str | None = None  # None = learned state is memory-only
    tick_seconds: float = 5.0  # supervisor cadence
    autonomy: str = "suggest"  # observe | suggest | act
    playbooks_path: str | None = None
    playbook_dry_run: bool = False
    max_actions_per_hour: int = 6
    agent_token: str | None = None  # required for privileged agent tools
    allowed_origins: tuple[str, ...] = ()
    adaptive: bool = True  # learn per-service stale thresholds
    adaptive_floor_seconds: float = 15.0
    adaptive_ceiling_seconds: float = 3600.0
    learn_roster_after: int = 20  # reports before an unregistered peer is "expected"
    forget_after_seconds: float = 86400.0
    event_buffer: int = 200
    report_replay: bool = True

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
            state_dir=default_state_dir(),
            tick_seconds=_env_float("LISTENER55_TICK", 5.0),
            autonomy=(_env("LISTENER55_AUTONOMY", "suggest") or "suggest").strip().lower(),
            playbooks_path=_env("LISTENER55_PLAYBOOKS"),
            playbook_dry_run=_env_bool("LISTENER55_PLAYBOOK_DRY_RUN", False),
            max_actions_per_hour=_env_int("LISTENER55_MAX_ACTIONS_PER_HOUR", 6),
            agent_token=_env("LISTENER55_AGENT_TOKEN"),
            allowed_origins=tuple(
                o.strip().rstrip("/")
                for o in (_env("LISTENER55_ALLOWED_ORIGINS", "") or "").split(",")
                if o.strip()
            ),
            adaptive=_env_bool("LISTENER55_ADAPTIVE", True),
            adaptive_floor_seconds=_env_float("LISTENER55_ADAPTIVE_FLOOR", 15.0),
            adaptive_ceiling_seconds=_env_float("LISTENER55_ADAPTIVE_CEILING", 3600.0),
            learn_roster_after=_env_int("LISTENER55_LEARN_ROSTER_AFTER", 20),
            forget_after_seconds=_env_float("LISTENER55_FORGET_AFTER", 86400.0),
            event_buffer=_env_int("LISTENER55_EVENT_BUFFER", 200),
            report_replay=_env_bool("LISTENER55_REPORT_REPLAY", True),
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
        if self.tick_seconds <= 0:
            raise ValueError("LISTENER55_TICK must be > 0")
        if self.autonomy not in AUTONOMY_LEVELS:
            raise ValueError(
                f"LISTENER55_AUTONOMY must be one of {', '.join(AUTONOMY_LEVELS)}"
            )
        if self.max_actions_per_hour < 0:
            raise ValueError("LISTENER55_MAX_ACTIONS_PER_HOUR must be >= 0")
        if self.agent_token is not None and len(self.agent_token) < 16:
            raise ValueError("LISTENER55_AGENT_TOKEN must be at least 16 characters")
        if not (0 < self.adaptive_floor_seconds <= self.adaptive_ceiling_seconds):
            raise ValueError("need 0 < LISTENER55_ADAPTIVE_FLOOR <= LISTENER55_ADAPTIVE_CEILING")
        if self.learn_roster_after < 1:
            raise ValueError("LISTENER55_LEARN_ROSTER_AFTER must be >= 1")
        if self.forget_after_seconds <= 0:
            raise ValueError("LISTENER55_FORGET_AFTER must be > 0")
        if self.event_buffer < 1:
            raise ValueError("LISTENER55_EVENT_BUFFER must be >= 1")

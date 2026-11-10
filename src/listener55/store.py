"""Durable state: what the service has learned survives a restart.

One JSON document, written atomically. The store is deliberately paranoid
about its own file, because a service that cannot boot on a truncated state
file has turned a memory aid into a single point of failure:

*   writes go to a temp file, are fsynced, then renamed over the original, so a
    crash mid-write leaves the previous good copy in place;
*   a file that will not parse, or that was written by a newer incompatible
    version, is moved aside (``state.json.quarantined-<ts>``) and the service
    starts with empty state rather than refusing to start;
*   a state directory that cannot be created or written downgrades the store to
    memory-only and says so.

Stdlib only.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from pathlib import Path
from typing import Any

log = logging.getLogger("listener55.store")

STATE_VERSION = 1
STATE_FILE = "state.json"
AUDIT_FILE = "audit.jsonl"
#: Rotate the audit trail at this size; one previous generation is kept.
AUDIT_MAX_BYTES = 2_000_000


class StateStore:
    """Sectioned key/value document with atomic persistence."""

    def __init__(self, state_dir: str | os.PathLike[str] | None = None) -> None:
        self._lock = threading.RLock()
        self._data: dict[str, Any] = {}
        self._dirty = False
        self.dir: Path | None = Path(state_dir) if state_dir else None
        #: Things the store did to keep itself working, for the heal log.
        self.notes: list[str] = []
        self.last_saved_at: float | None = None
        self.save_failures = 0
        if self.dir is not None:
            self._open()

    # ------------------------------------------------------------ lifecycle

    def _open(self) -> None:
        assert self.dir is not None
        try:
            self.dir.mkdir(parents=True, exist_ok=True)
            probe = self.dir / ".write-probe"
            probe.write_text("ok", encoding="utf-8")
            probe.unlink()
        except OSError as exc:
            self.notes.append(f"state dir unusable ({exc}); running memory-only")
            log.warning("state dir %s unusable (%s); running memory-only", self.dir, exc)
            self.dir = None
            return
        path = self.dir / STATE_FILE
        if not path.exists():
            return
        try:
            doc = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(doc, dict):
                raise ValueError("state root is not an object")
            version = doc.get("state_version")
            if version != STATE_VERSION:
                raise ValueError(f"unsupported state_version {version!r}")
            sections = doc.get("sections")
            if not isinstance(sections, dict):
                raise ValueError("state has no sections object")
            self._data = sections
        except (OSError, ValueError) as exc:  # JSONDecodeError is a ValueError
            self._quarantine(path, str(exc))

    def _quarantine(self, path: Path, reason: str) -> None:
        target = path.with_name(f"{path.name}.quarantined-{int(time.time())}")
        try:
            os.replace(path, target)
            note = f"state file unreadable ({reason}); moved to {target.name}, starting fresh"
        except OSError as exc:
            note = f"state file unreadable ({reason}) and could not be moved ({exc}); starting fresh"
        self.notes.append(note)
        log.warning(note)
        self._data = {}

    @property
    def persistent(self) -> bool:
        return self.dir is not None

    # --------------------------------------------------------------- access

    def section(self, name: str) -> dict[str, Any]:
        """The live dict for one section. Mutate it, then call ``touch()``."""
        with self._lock:
            sec = self._data.get(name)
            if not isinstance(sec, dict):
                sec = {}
                self._data[name] = sec
            return sec

    def touch(self) -> None:
        with self._lock:
            self._dirty = True

    @property
    def lock(self) -> threading.RLock:
        return self._lock

    # ----------------------------------------------------------------- save

    def save(self, force: bool = False) -> bool:
        """Persist if dirty. Returns True when the on-disk copy is current."""
        if self.dir is None:
            return False
        with self._lock:
            if not (self._dirty or force):
                return True
            body = None
            # Sections are mutated by other threads under their own locks, so
            # a dict can change size mid-serialisation. That is a retry, not a
            # failure: the next attempt sees a settled dict.
            for _attempt in range(5):
                try:
                    body = json.dumps(
                        {
                            "state_version": STATE_VERSION,
                            "saved_at": time.time(),
                            "sections": self._data,
                        },
                        separators=(",", ":"),
                        default=str,
                    )
                    break
                except RuntimeError:
                    continue
                except (TypeError, ValueError) as exc:
                    self.save_failures += 1
                    log.error("state not serialisable: %s", exc)
                    return False
            if body is None:
                return False  # still dirty; the next cycle tries again
            self._dirty = False
        path = self.dir / STATE_FILE
        tmp = path.with_name(f"{path.name}.tmp-{os.getpid()}")
        try:
            with open(tmp, "w", encoding="utf-8") as fh:
                fh.write(body)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, path)
        except OSError as exc:
            self.save_failures += 1
            with self._lock:
                self._dirty = True  # try again next cycle
            log.warning("state save failed: %s", exc)
            try:
                tmp.unlink()
            except OSError:
                pass
            return False
        self.last_saved_at = time.time()
        return True

    # ---------------------------------------------------------------- audit

    def append_audit(self, record: dict[str, Any]) -> None:
        """Append one line to the audit trail. Best effort, never raises."""
        if self.dir is None:
            return
        path = self.dir / AUDIT_FILE
        try:
            if path.exists() and path.stat().st_size > AUDIT_MAX_BYTES:
                os.replace(path, path.with_name(AUDIT_FILE + ".1"))
            with open(path, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(record, separators=(",", ":"), default=str) + "\n")
        except OSError as exc:
            log.warning("audit append failed: %s", exc)

    def describe(self) -> dict[str, Any]:
        return {
            "persistent": self.persistent,
            "dir": str(self.dir) if self.dir else None,
            "last_saved_at": self.last_saved_at,
            "save_failures": self.save_failures,
            "notes": list(self.notes),
        }

"""Self-report delivery: JSON POST and/or local JSONL log."""

from __future__ import annotations

import json
import logging
import threading
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Callable

from listener55.schema import Metrics, assert_valid

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

    def build(self) -> dict[str, Any]:
        payload = self.metrics.build_payload(
            instance_id=self.instance_id,
            host=self.host,
            port=self.port,
        )
        return assert_valid(payload)

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
            try:
                status, _ = self._post_fn(self.report_url, body, self.http_timeout)
                if status < 200 or status >= 300:
                    ok = False
                    errors.append(f"http status {status}")
            except (urllib.error.URLError, TimeoutError, OSError, ValueError) as exc:
                ok = False
                errors.append(f"http:{exc}")

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
                "User-Agent": "listener-5.5/0.1",
            },
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return int(resp.status), resp.read().decode("utf-8", errors="replace")

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._loop, name="listener55-reporter", daemon=True
        )
        self._thread.start()

    def stop(self, timeout: float = 2.0) -> None:
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

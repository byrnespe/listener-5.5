"""HTTP listener server (stdlib only)."""

from __future__ import annotations

import json
import logging
import random
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import urlparse

from listener55.config import Config
from listener55.reporter import SelfReporter
from listener55.schema import Metrics

log = logging.getLogger("listener55.server")


class ListenerState:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.metrics = Metrics()
        self.reporter = SelfReporter(
            metrics=self.metrics,
            instance_id=config.instance_id,
            host=config.host,
            port=config.port,
            interval_seconds=config.report_interval_seconds,
            report_url=config.report_url,
            report_log_path=config.report_log_path,
        )
        self._httpd: ThreadingHTTPServer | None = None

    def process_item(self, item: Any) -> dict[str, Any]:
        """Process one inbound event. Pure-ish; mutates metrics."""
        self.metrics.mark_received()
        try:
            if self.config.process_fail_rate > 0 and random.random() < self.config.process_fail_rate:
                raise RuntimeError("injected process failure")
            # Normalize to a dict event envelope.
            if item is None:
                raise ValueError("empty payload")
            if isinstance(item, (bytes, bytearray)):
                item = item.decode("utf-8")
            if isinstance(item, str):
                text = item.strip()
                if not text:
                    raise ValueError("empty payload")
                try:
                    item = json.loads(text)
                except json.JSONDecodeError:
                    item = {"raw": text}
            if not isinstance(item, (dict, list, str, int, float, bool)):
                raise TypeError(f"unsupported payload type: {type(item).__name__}")
            result = {
                "ok": True,
                "echo_type": type(item).__name__,
                "size": len(json.dumps(item, default=str)),
            }
            self.metrics.mark_ok()
            return result
        except Exception as exc:  # noqa: BLE001 — surface as processed_error
            self.metrics.mark_error(str(exc))
            return {"ok": False, "error": str(exc)}


def make_handler(state: ListenerState):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"
        server_version = "listener-5.5/0.1"

        def log_message(self, fmt: str, *args: Any) -> None:
            log.debug("http " + fmt, *args)

        def _read_body(self) -> bytes:
            length = int(self.headers.get("Content-Length") or 0)
            if length < 0:
                raise ValueError("negative content-length")
            if length > state.config.max_body_bytes:
                raise ValueError("body too large")
            if length == 0:
                return b""
            return self.rfile.read(length)

        def _send_json(self, code: int, payload: dict[str, Any]) -> None:
            body = json.dumps(payload).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:  # noqa: N802
            path = urlparse(self.path).path
            if path in ("/healthz", "/health", "/"):
                counts = state.metrics.snapshot_counts()
                self._send_json(
                    200,
                    {
                        "status": state.metrics.status,
                        "service": "listener-5.5",
                        "instance_id": state.config.instance_id,
                        "uptime_seconds": round(state.metrics.uptime_seconds(), 3),
                        "counts": counts,
                    },
                )
                return
            if path == "/metrics":
                payload = state.metrics.build_payload(
                    instance_id=state.config.instance_id,
                    host=state.config.host,
                    port=state.config.port,
                )
                self._send_json(200, payload)
                return
            self._send_json(404, {"error": "not found"})

        def do_POST(self) -> None:  # noqa: N802
            path = urlparse(self.path).path
            if path not in ("/events", "/ingest", "/v1/events"):
                self._send_json(404, {"error": "not found"})
                return
            try:
                raw = self._read_body()
            except ValueError as exc:
                self._send_json(413 if "large" in str(exc) else 400, {"error": str(exc)})
                return
            if not raw:
                item: Any = None
            else:
                try:
                    item = json.loads(raw.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError):
                    item = raw
            result = state.process_item(item)
            code = 202 if result.get("ok") else 422
            self._send_json(code, result)

    return Handler


class ListenerService:
    """Owns HTTP server + self-reporter lifecycle."""

    def __init__(self, config: Config) -> None:
        config.validate()
        self.config = config
        self.state = ListenerState(config)
        self._thread: threading.Thread | None = None

    @property
    def metrics(self) -> Metrics:
        return self.state.metrics

    def start(self, blocking: bool = True) -> None:
        handler = make_handler(self.state)
        httpd = ThreadingHTTPServer((self.config.host, self.config.port), handler)
        # Reflect the actual bound port (port 0 support for tests).
        bound_host, bound_port = httpd.server_address[:2]
        self.state.reporter.host = bound_host if bound_host not in ("0.0.0.0", "::") else self.config.host
        self.state.reporter.port = int(bound_port)
        self.state._httpd = httpd
        self.metrics.set_status("healthy")
        self.state.reporter.start()
        log.info(
            "listening on %s:%s instance=%s report_interval=%ss",
            self.config.host,
            bound_port,
            self.config.instance_id,
            self.config.report_interval_seconds,
        )
        if blocking:
            try:
                httpd.serve_forever(poll_interval=0.5)
            finally:
                self.stop()
        else:
            self._thread = threading.Thread(
                target=httpd.serve_forever,
                kwargs={"poll_interval": 0.5},
                name="listener55-http",
                daemon=True,
            )
            self._thread.start()

    def stop(self) -> None:
        self.metrics.set_status("stopping")
        try:
            self.state.reporter.emit_once()
        except Exception:  # noqa: BLE001
            log.exception("final self-report failed")
        self.state.reporter.stop()
        if self.state._httpd is not None:
            self.state._httpd.shutdown()
            self.state._httpd.server_close()
            self.state._httpd = None
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2.0)

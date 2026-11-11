"""HTTP listener server (stdlib only)."""

from __future__ import annotations

import hmac
import json
import logging
import random
import threading
from collections import OrderedDict, deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

from listener55 import __version__
from listener55.agent import GUIDE, AgentAPI, ToolError
from listener55.brain import Brain
from listener55.config import Config
from listener55.fleet import FleetStore, Roster, resolve_registry_path
from listener55.heal import sd_notify
from listener55.reporter import SelfReporter
from listener55.schema import Metrics, utc_now_iso
from listener55.store import StateStore

DEAD_LETTER_MAX = 100
DEAD_LETTER_ITEM_BYTES = 8192
IDEMPOTENCY_KEYS = 2000
#: Seconds a connection may sit idle mid-request before its thread is freed.
#: Without it a client that connects and says nothing holds a thread forever.
REQUEST_TIMEOUT = 30
LOOPBACK_HOSTS = ("localhost", "127.0.0.1", "::1", "[::1]")

log = logging.getLogger("listener55.server")


class ListenerState:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.metrics = Metrics()
        # Learned state, incident memory and the undelivered-report spool all
        # live in one store so they survive a restart together.
        self.store = StateStore(config.state_dir)
        spool_section = self.store.section("spool")
        if not isinstance(spool_section.get("items"), list):
            spool_section["items"] = []
        self.reporter = SelfReporter(
            metrics=self.metrics,
            instance_id=config.instance_id,
            host=config.host,
            port=config.port,
            interval_seconds=config.report_interval_seconds,
            report_url=config.report_url,
            report_log_path=config.report_log_path,
            auth_token=config.report_url_token,
            spool=spool_section["items"],
            on_spool_change=self.store.touch,
            replay=config.report_replay,
            meta_fn=lambda: self.brain.meta(),
        )
        self._events: deque[dict[str, Any]] = deque(maxlen=config.event_buffer)
        self._dead: deque[dict[str, Any]] = deque(maxlen=DEAD_LETTER_MAX)
        self._seen_keys: OrderedDict[str, None] = OrderedDict()
        self._journal_lock = threading.Lock()
        self._httpd: ThreadingHTTPServer | None = None
        # The fleet bus: other services POST their self-reports to
        # /self-report and /fleet rolls them up against the org registry.
        # A missing registry is not fatal -- the bus still aggregates, it just
        # cannot say which services are missing.
        self.fleet = FleetStore(
            roster=Roster(resolve_registry_path(config.registry_path)),
            stale_after=config.fleet_stale_after_seconds,
        )
        self.brain = Brain(
            config=config,
            metrics=self.metrics,
            fleet=self.fleet,
            reporter=self.reporter,
            store=self.store,
        )
        self.agent = AgentAPI(self)

    def own_report(self) -> dict[str, Any]:
        """This service's own self-report, carrying its registry id."""
        # Every health check doubles as a check on the supervisor: it watches
        # the workers, and their traffic watches it.
        self.brain.ensure_running()
        return self.brain.own_report()

    def metrics_payload(self) -> dict[str, Any]:
        return self.metrics.build_payload(
            instance_id=self.config.instance_id,
            host=self.reporter.host,
            port=self.reporter.port,
        )

    def ingest_own_report(self) -> None:
        """Fold this service's current state into the fleet store.

        Done at read time rather than by POSTing to ourselves: the bus should
        never be missing from its own roster, and a loopback round trip to
        achieve that would be silly.
        """
        self.fleet.ingest(self.own_report(), observe=False)

    def prometheus(self) -> str:
        """The same numbers as /metrics and /fleet, in Prometheus text format."""
        counts = self.metrics.snapshot_counts()
        self.ingest_own_report()
        roll = self.fleet.rollup()
        brain = self.brain
        lines: list[str] = []

        def metric(name: str, kind: str, help_text: str, samples: list[tuple[str, float]]) -> None:
            lines.append(f"# HELP listener55_{name} {help_text}")
            lines.append(f"# TYPE listener55_{name} {kind}")
            for labels, value in samples:
                lines.append(f"listener55_{name}{labels} {value:g}")

        metric("events_received_total", "counter", "Events received.", [("", counts["received"])])
        metric("events_processed_total", "counter", "Events processed, by outcome.", [
            ('{outcome="ok"}', counts["processed_ok"]),
            ('{outcome="error"}', counts["processed_error"]),
        ])
        metric("events_deduplicated_total", "counter", "Retried events acknowledged without reprocessing.", [("", self.metrics.deduplicated)])
        metric("self_reports_total", "counter", "Self-reports emitted, by outcome.", [
            ('{outcome="sent"}', counts["self_reports_sent"]),
            ('{outcome="failed"}', counts["self_reports_failed"]),
        ])
        metric("self_reports_spooled", "gauge", "Undelivered self-reports waiting for replay.", [("", self.reporter.spooled())])
        metric("uptime_seconds", "gauge", "Seconds since start.", [("", round(self.metrics.uptime_seconds(), 3))])
        metric("up", "gauge", "1 if this service considers itself healthy.", [("", 1 if self.metrics.status == "healthy" else 0)])
        metric("internal_errors_total", "counter", "Requests that hit an unexpected exception.", [("", self.metrics.internal_errors)])
        metric("self_heals_total", "counter", "Times the service repaired something in itself.", [("", brain.heals)])
        metric("fleet_services", "gauge", "Services on the fleet bus, by state.", [
            (f'{{state="{st}"}}', n) for st, n in roll["summary"].items() if st != "total"
        ])
        metric("fleet_ok", "gauge", "1 if every expected service is healthy.", [("", 1 if roll["status"] == "ok" else 0)])
        metric("incidents_open", "gauge", "Incidents currently open.", [("", len(brain.incidents.open_incidents()))])
        metric("agent_tool_calls_total", "counter", "Tool calls made through the agent interface.", [("", self.agent.calls)])
        return "\n".join(lines) + "\n"

    # ------------------------------------------------------------- journal

    def _journal(self, item: Any, result: dict[str, Any]) -> None:
        entry = {
            "at": utc_now_iso(),
            "ok": bool(result.get("ok")),
            "type": result.get("echo_type") or type(item).__name__,
            "size": result.get("size"),
        }
        if isinstance(item, dict) and isinstance(item.get("type"), str):
            entry["event_type"] = item["type"][:80]
        with self._journal_lock:
            self._events.append(entry)
            if not entry["ok"]:
                entry["error"] = result.get("error")
                try:
                    kept: Any = item if isinstance(item, (bytes, bytearray)) else json.dumps(item)
                    if isinstance(kept, (bytes, bytearray)):
                        kept = kept.decode("utf-8", errors="replace")
                except (TypeError, ValueError):
                    kept = repr(item)
                self._dead.append(
                    {
                        "at": entry["at"],
                        "error": result.get("error"),
                        "truncated": len(kept) > DEAD_LETTER_ITEM_BYTES,
                        "item": kept[:DEAD_LETTER_ITEM_BYTES],
                    }
                )

    def recent_events(self, limit: int = 20, failed_only: bool = False) -> dict[str, Any]:
        with self._journal_lock:
            events = [e for e in self._events if not failed_only or not e["ok"]][-limit:]
            dead = list(self._dead)[-limit:]
            return {
                "events": events,
                "dead_letters": dead,
                "dead_letter_count": len(self._dead),
                "deduplicated": self.metrics.deduplicated,
            }

    def replay_dead_letters(self, limit: int = 50) -> dict[str, Any]:
        """Reprocess dead-lettered events. Truncated ones cannot be replayed
        faithfully and are left where they are."""
        with self._journal_lock:
            batch = [d for d in self._dead if not d["truncated"]][:limit]
            for d in batch:
                self._dead.remove(d)
        ok = 0
        for d in batch:
            # A replay that fails again lands back in the queue by itself.
            if self.process_item(d["item"]).get("ok"):
                ok += 1
        return {"replayed": len(batch), "succeeded": ok, "remaining": len(self._dead)}

    def process_item(self, item: Any, idempotency_key: str | None = None) -> dict[str, Any]:
        """Process one inbound event. Pure-ish; mutates metrics."""
        if idempotency_key:
            key = idempotency_key[:200]
            with self._journal_lock:
                if key in self._seen_keys:
                    # A sender retrying after a timeout must not double-count.
                    self.metrics.mark_deduplicated()
                    return {"ok": True, "duplicate": True}
                self._seen_keys[key] = None
                while len(self._seen_keys) > IDEMPOTENCY_KEYS:
                    self._seen_keys.popitem(last=False)
        original = item
        result = self._process(item)
        if not result.get("ok") and idempotency_key:
            with self._journal_lock:
                # Only successes are remembered, so a failed attempt can be retried.
                self._seen_keys.pop(idempotency_key[:200], None)
        self._journal(original, result)
        return result

    def _process(self, item: Any) -> dict[str, Any]:
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
        server_version = f"listener-5.5/{__version__}"
        timeout = REQUEST_TIMEOUT

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

        def _send(self, code: int, body: bytes, ctype: str, extra: dict[str, str] | None = None) -> None:
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Connection", "close")
            for k, v in (extra or {}).items():
                self.send_header(k, v)
            self.end_headers()
            self.wfile.write(body)

        def _send_json(self, code: int, payload: Any, extra: dict[str, str] | None = None) -> None:
            self._send(code, json.dumps(payload, default=str).encode("utf-8"), "application/json", extra)

        def _send_text(self, code: int, text: str, ctype: str = "text/plain; charset=utf-8") -> None:
            self._send(code, text.encode("utf-8"), ctype)

        # ------------------------------------------------------- agent auth

        def _authorized(self) -> bool:
            token = state.config.agent_token
            if not token:
                return False
            header = self.headers.get("Authorization") or ""
            scheme, _, given = header.partition(" ")
            if scheme.lower() != "bearer":
                return False
            # Constant-time: the comparison must not leak how much matched.
            return hmac.compare_digest(given.strip().encode("utf-8"), token.encode("utf-8"))

        def _actor(self) -> str:
            name = (self.headers.get("X-Agent-Name") or "").strip()
            return (name or "anonymous")[:80]

        def _origin_ok(self) -> bool:
            """Refuse cross-site browser requests to the agent endpoints.

            A page open in someone's browser can POST to 127.0.0.1. Agents
            and CLIs send no Origin header; browsers always do, so an Origin
            that is neither loopback nor explicitly allowed is turned away.
            """
            origin = self.headers.get("Origin")
            if not origin:
                return True
            if origin.rstrip("/") in state.config.allowed_origins:
                return True
            try:
                host = urlparse(origin).hostname or ""
            except ValueError:
                return False
            return host in LOOPBACK_HOSTS

        def _tool_error(self, exc: ToolError) -> None:
            extra = {"WWW-Authenticate": "Bearer"} if exc.http == 401 else None
            self._send_json(
                exc.http, {"ok": False, "error": {"code": exc.code, "message": exc.message}}, extra
            )

        # -------------------------------------------------------------- GET

        def do_GET(self) -> None:  # noqa: N802
            try:
                self._get()
            except (BrokenPipeError, ConnectionResetError):
                raise
            except Exception as exc:  # noqa: BLE001 -- one bad request must not look like an outage
                log.exception("GET %s crashed", self.path)
                state.metrics.mark_internal_error()
                self._send_json(500, {"error": "internal error", "detail": type(exc).__name__})

        def _get(self) -> None:
            parsed = urlparse(self.path)
            path = parsed.path
            query = parse_qs(parsed.query)
            if path in ("/healthz", "/health", "/"):
                # Contract-shaped, per the org self-report schema. This is a
                # superset of what this endpoint used to return, so existing
                # callers keep working.
                self._send_json(200, state.own_report())
                return
            if path == "/livez":
                # Deliberately trivial: "is the socket being served", nothing
                # else. The supervisor probes this to decide whether to keep
                # telling systemd the process is worth keeping.
                self._send_json(200, {"ok": True})
                return
            if path == "/readyz":
                status = state.metrics.status
                ready = status in ("healthy", "degraded")
                self._send_json(200 if ready else 503, {"ready": ready, "status": status})
                return
            if path == "/metrics":
                payload = state.metrics.build_payload(
                    instance_id=state.config.instance_id,
                    host=state.config.host,
                    port=state.config.port,
                )
                self._send_json(200, payload)
                return
            if path == "/metrics/prometheus":
                self._send_text(200, state.prometheus(), "text/plain; version=0.0.4; charset=utf-8")
                return
            if path == "/fleet":
                state.ingest_own_report()
                # Always 200: /fleet is a report about the fleet, not this
                # service's own health. Returning 503 whenever any peer is
                # unwell would make the endpoint useless as a data source and
                # would flap the bus's own monitoring.
                self._send_json(200, state.fleet.rollup())
                return
            if path.startswith("/fleet/"):
                key = path[len("/fleet/") :].strip("/")
                if key == state.config.registry_id:
                    state.ingest_own_report()
                row = state.fleet.one(key) if key else None
                if row is None:
                    self._send_json(
                        404,
                        {
                            "error": "unknown service",
                            "detail": "not reporting and not in the registry roster",
                            "id": key,
                        },
                    )
                    return
                self._send_json(200, row)
                return
            if path == "/incidents":
                status = (query.get("status") or ["open"])[0]
                if status not in ("open", "resolved", "all"):
                    self._send_json(400, {"error": "status must be open, resolved or all"})
                    return
                self._send_json(
                    200,
                    {
                        "incidents": state.brain.incidents.list(status, (query.get("service") or [None])[0], 100),
                        "stats": state.brain.incidents.stats(),
                    },
                )
                return
            if path == "/insights":
                self._send_json(200, state.brain.insights())
                return
            if path in ("/agent", "/agent/manifest"):
                self._send_json(200, state.agent.manifest())
                return
            if path == "/agent/tools":
                fmt = (query.get("format") or ["neutral"])[0]
                try:
                    self._send_json(200, {"format": fmt, "tools": state.agent.catalog(fmt)})
                except ToolError as exc:
                    self._tool_error(exc)
                return
            if path in ("/agent/guide", "/llms.txt"):
                self._send_text(200, GUIDE, "text/markdown; charset=utf-8")
                return
            if path == "/openapi.json":
                self._send_json(200, state.agent.openapi())
                return
            if path == "/mcp":
                # No server-initiated stream is offered; say so the way the
                # MCP HTTP transport expects.
                self._send_json(405, {"error": "POST JSON-RPC to /mcp"}, {"Allow": "POST"})
                return
            self._send_json(404, {"error": "not found"})

        # ------------------------------------------------------------- POST

        def do_POST(self) -> None:  # noqa: N802
            try:
                self._post()
            except (BrokenPipeError, ConnectionResetError):
                raise
            except Exception as exc:  # noqa: BLE001
                log.exception("POST %s crashed", self.path)
                state.metrics.mark_internal_error()
                self._send_json(500, {"error": "internal error", "detail": type(exc).__name__})

        def _post(self) -> None:
            path = urlparse(self.path).path
            is_agent = path == "/mcp" or path == "/agent/call" or path.startswith("/agent/tools/")
            if not is_agent and path not in ("/events", "/ingest", "/v1/events", "/self-report"):
                self._send_json(404, {"error": "not found"})
                return
            try:
                raw = self._read_body()
            except ValueError as exc:
                self._send_json(413 if "large" in str(exc) else 400, {"error": str(exc)})
                return

            if is_agent:
                self._post_agent(path, raw)
                return

            # Browsers always send Origin on a cross-site POST; services and
            # CLIs do not. A page must not be able to feed the bus.
            if not self._origin_ok():
                self._send_json(403, {"ok": False, "error": "origin not allowed"})
                return

            if path == "/self-report":
                expected = state.config.report_token
                if expected:
                    scheme, _, given = (self.headers.get("Authorization") or "").partition(" ")
                    if scheme.lower() != "bearer" or not hmac.compare_digest(
                        given.strip().encode("utf-8"), expected.encode("utf-8")
                    ):
                        self._send_json(
                            401,
                            {"ok": False, "error": "self-reports need Authorization: Bearer <report token>"},
                            {"WWW-Authenticate": "Bearer"},
                        )
                        return
                try:
                    report = json.loads(raw.decode("utf-8")) if raw else None
                except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                    self._send_json(400, {"error": f"invalid JSON: {exc}"})
                    return
                accepted, errors = state.fleet.ingest(report)
                if not accepted:
                    # Reject rather than store: a bus holding malformed health
                    # is worse than one holding none.
                    self._send_json(
                        422, {"ok": False, "error": "invalid self-report", "errors": errors}
                    )
                    return
                self._send_json(
                    202, {"ok": True, "id": state.fleet.key_for(report)}
                )
                return
            if not raw:
                item: Any = None
            else:
                try:
                    item = json.loads(raw.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError):
                    item = raw
            result = state.process_item(item, idempotency_key=self.headers.get("Idempotency-Key"))
            if result.get("duplicate"):
                self._send_json(200, result)
                return
            code = 202 if result.get("ok") else 422
            self._send_json(code, result)

        def _post_agent(self, path: str, raw: bytes) -> None:
            if not self._origin_ok():
                self._send_json(403, {"ok": False, "error": {"code": "forbidden", "message": "origin not allowed"}})
                return
            try:
                body = json.loads(raw.decode("utf-8")) if raw.strip() else None
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                if path == "/mcp":
                    self._send_json(
                        400, {"jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": f"parse error: {exc}"}}
                    )
                else:
                    self._send_json(400, {"ok": False, "error": {"code": "invalid_json", "message": str(exc)}})
                return
            authorized, actor = self._authorized(), self._actor()

            if path == "/mcp":
                response = state.agent.mcp(body, authorized=authorized, actor=actor, transport="mcp-http")
                if response is None:
                    self._send(202, b"", "application/json")
                else:
                    self._send_json(200, response)
                return

            if path == "/agent/call":
                # One endpoint for clients that route every tool call the same
                # way. Accepts {"tool"|"name": ..., "arguments": {...} | "<json>"}.
                if not isinstance(body, dict):
                    self._send_json(
                        422, {"ok": False, "error": {"code": "invalid_arguments", "message": "body must be an object"}}
                    )
                    return
                fn = body.get("function") if isinstance(body.get("function"), dict) else body
                name = fn.get("tool") or fn.get("name")
                arguments = fn.get("arguments", fn.get("input"))
            else:
                name = path[len("/agent/tools/") :].strip("/")
                arguments = body
            try:
                result = state.agent.call(str(name), arguments, authorized=authorized, actor=actor)
            except ToolError as exc:
                self._tool_error(exc)
                return
            self._send_json(200, {"ok": True, "tool": name, "result": result})

    return Handler


class ListenerService:
    """Owns HTTP server + self-reporter lifecycle."""

    def __init__(self, config: Config) -> None:
        config.validate()
        self.config = config
        self.state = ListenerState(config)
        self._thread: threading.Thread | None = None
        self._stop_lock = threading.Lock()
        self._stopped = False

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
        probe_host = "127.0.0.1" if bound_host in ("0.0.0.0", "", "::") else bound_host
        if ":" in probe_host:
            probe_host = f"[{probe_host}]"
        self.state.brain.probe_url = f"http://{probe_host}:{bound_port}/livez"
        self.state.brain.start()
        if self.state.brain.autonomy == "act" and not self.config.report_token:
            log.warning(
                "autonomy is 'act' but LISTENER55_REPORT_TOKEN is unset: anyone who can reach "
                "this port can send a self-report, and self-reports can trigger playbooks"
            )
        sd_notify("READY=1")
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

    def request_stop(self) -> None:
        """Ask a blocking ``start()`` to return. Safe from a signal handler.

        ``httpd.shutdown()`` waits for ``serve_forever`` to exit. A signal
        handler runs *on* the thread inside ``serve_forever``, so calling it
        there waits on itself forever -- the service would ignore SIGTERM
        until systemd gave up and sent SIGKILL. Shutting down from another
        thread lets the serve loop finish and run its own cleanup.
        """
        httpd = self.state._httpd
        if httpd is not None:
            threading.Thread(target=httpd.shutdown, name="listener55-shutdown", daemon=True).start()

    def stop(self) -> None:
        with self._stop_lock:
            if self._stopped:
                return
            self._stopped = True
        sd_notify("STOPPING=1")
        self.metrics.set_status("stopping")
        self.state.brain.stop()
        try:
            self.state.reporter.emit_once()
        except Exception:  # noqa: BLE001
            log.exception("final self-report failed")
        self.state.reporter.stop()
        self.state.store.save(force=True)
        if self.state._httpd is not None:
            self.state._httpd.shutdown()
            self.state._httpd.server_close()
            self.state._httpd = None
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2.0)

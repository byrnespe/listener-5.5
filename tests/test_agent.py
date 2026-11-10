"""Tests for the agent interface: tool catalogs in every dialect, plain HTTP
invocation, MCP over HTTP and stdio, authorisation, and the hardening around
them. Everything runs against a real server on a loopback port."""

from __future__ import annotations

import io
import json
import os
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

import pytest

from listener55.agent import FORMATS, MCP_VERSIONS, run_stdio_bridge
from listener55.config import Config
from listener55.server import ListenerService

TOKEN = "s3cret-token-0123456789"

PLAYBOOKS = {
    "playbooks": [
        {
            "name": "restart",
            "description": "restart the unit",
            "match": {"service": "*", "kinds": ["stale", "unknown"]},
            "action": {"type": "command", "argv": ["true", "{service}"]},
            "cooldown_seconds": 0,
            "allow_unregistered": True,
        }
    ]
}

REGISTRY = {
    "schema_version": "1.0",
    "services": [
        {"id": "alpha", "name": "Alpha", "status": "live", "self_report": True, "port": 9001},
        {"id": "listener-55", "name": "Bus", "status": "live", "self_report": True, "port": 8555},
    ],
}


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def report(registry_id="alpha", **over):
    body = {
        "schema_version": "1.0",
        "service": registry_id,
        "registry_id": registry_id,
        "instance_id": "i-1",
        "status": "healthy",
        "uptime_seconds": 1.0,
        "started_at": "2026-09-20T05:00:00.000Z",
        "reported_at": "2026-09-20T05:00:01.000Z",
    }
    body.update(over)
    return body


def start(tmp_path: Path, **over) -> tuple[ListenerService, str]:
    (tmp_path / "registry.json").write_text(json.dumps(REGISTRY), encoding="utf-8")
    (tmp_path / "playbooks.json").write_text(json.dumps(PLAYBOOKS), encoding="utf-8")
    port = free_port()
    cfg = dict(
        host="127.0.0.1",
        port=port,
        report_url=None,
        report_interval_seconds=3600,
        report_log_path=tmp_path / "sr.jsonl",
        instance_id="agent-test",
        max_body_bytes=65536,
        process_fail_rate=0.0,
        registry_path=str(tmp_path / "registry.json"),
        playbooks_path=str(tmp_path / "playbooks.json"),
        tick_seconds=3600,  # ticks are driven by hand
        agent_token=TOKEN,
        state_dir=str(tmp_path / "state"),
    )
    cfg.update(over)
    svc = ListenerService(Config(**cfg))
    svc.state.brain.grace_seconds = 0
    svc.start(blocking=False)
    base = f"http://127.0.0.1:{port}"
    deadline = time.time() + 3
    while time.time() < deadline:
        try:
            urllib.request.urlopen(base + "/livez", timeout=0.5).close()
            return svc, base
        except Exception:
            time.sleep(0.02)
    pytest.fail("server did not become ready")


@pytest.fixture()
def live(tmp_path):
    svc, base = start(tmp_path)
    try:
        yield svc, base
    finally:
        svc.stop()


def http(base, path, body=None, *, token=None, headers=None, method=None):
    """(status, parsed-json-or-text, headers)"""
    hdrs = dict(headers or {})
    data = None
    if body is not None:
        data = body if isinstance(body, bytes) else json.dumps(body).encode()
        hdrs.setdefault("Content-Type", "application/json")
    if token:
        hdrs["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(base + path, data=data, headers=hdrs, method=method or ("POST" if data is not None else "GET"))
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            raw, status, rh = resp.read(), resp.status, resp.headers
    except urllib.error.HTTPError as exc:
        raw, status, rh = exc.read(), exc.code, exc.headers
    text = raw.decode()
    try:
        return status, json.loads(text) if text else None, rh
    except json.JSONDecodeError:
        return status, text, rh


def tool(base, name, args=None, **kw):
    return http(base, f"/agent/tools/{name}", args or {}, **kw)


def rpc(base, method, params=None, rid=1, **kw):
    msg = {"jsonrpc": "2.0", "id": rid, "method": method}
    if params is not None:
        msg["params"] = params
    return http(base, "/mcp", msg, **kw)


def make_stale(svc, key="alpha"):
    svc.state.fleet.ingest(report(key))
    svc.state.fleet._reports[key]["received_mono"] -= 500
    svc.state.brain.tick()
    return svc.state.brain.incidents.get_open(key, "stale")


# ---------------------------------------------------------------- discovery


def test_manifest_describes_every_way_in(live):
    _, base = live
    status, doc, _ = http(base, "/agent")
    assert status == 200
    assert doc["model_agnostic"] is True
    assert doc["interfaces"]["mcp_http"]["path"] == "/mcp"
    assert doc["interfaces"]["catalog_formats"] == list(FORMATS)
    assert doc["auth"]["privileged_tools"] == "bearer"
    assert "run_playbook" in doc["tools"]["privileged"]


def test_every_dialect_lists_the_same_tools(live):
    svc, base = live
    expected = sorted(svc.state.agent.tools)
    names = {}
    for fmt in FORMATS:
        status, doc, _ = http(base, f"/agent/tools?format={fmt}")
        assert status == 200, fmt
        tools = doc["tools"]
        if fmt == "openai":
            assert all(t["type"] == "function" for t in tools)
            names[fmt] = [t["function"]["name"] for t in tools]
            assert tools[0]["function"]["parameters"]["type"] == "object"
        elif fmt == "gemini":
            decls = tools[0]["functionDeclarations"]
            names[fmt] = [t["name"] for t in decls]
            assert "additionalProperties" not in json.dumps(decls)
        elif fmt == "anthropic":
            names[fmt] = [t["name"] for t in tools]
            assert all(set(t) == {"name", "description", "input_schema"} for t in tools)
        elif fmt == "mcp":
            names[fmt] = [t["name"] for t in tools]
            assert all("inputSchema" in t for t in tools)
        else:
            names[fmt] = [t["name"] for t in tools]
            assert all(t["invoke"]["path"] == f"/agent/tools/{t['name']}" for t in tools)
    for fmt, got in names.items():
        assert sorted(got) == expected, fmt


def test_unknown_catalog_format_is_a_400(live):
    _, base = live
    assert http(base, "/agent/tools?format=klingon")[0] == 400


def test_tool_names_are_valid_function_names_everywhere(live):
    import re

    svc, _ = live
    for name, t in svc.state.agent.tools.items():
        assert re.fullmatch(r"[a-zA-Z0-9_-]{1,64}", name)
        assert len(t.description) > 20


def test_guide_and_openapi_are_served(live):
    svc, base = live
    status, text, headers = http(base, "/agent/guide")
    assert status == 200 and "fleet_status" in text
    assert headers["Content-Type"].startswith("text/markdown")
    assert http(base, "/llms.txt")[1] == text
    status, doc, _ = http(base, "/openapi.json")
    assert doc["openapi"].startswith("3.1")
    for name, t in svc.state.agent.tools.items():
        op = doc["paths"][f"/agent/tools/{name}"]["post"]
        assert op["operationId"] == name
        assert ("security" in op) == t.privileged


# --------------------------------------------------------------- plain HTTP


def test_read_tools_need_no_token(live):
    _, base = live
    status, body, _ = tool(base, "fleet_status")
    assert status == 200 and body["ok"] is True
    assert body["result"]["registry"]["loaded"] is True
    assert "autonomy" in body["result"]


def test_fleet_status_can_trim_to_what_needs_attention(live):
    svc, base = live
    svc.state.fleet.ingest(report("alpha"))
    _, body, _ = tool(base, "fleet_status", {"only_attention": True})
    assert body["result"]["services"] == []
    assert body["result"]["summary"]["healthy"] == 2


def test_privileged_tool_without_a_token_is_401(live):
    _, base = live
    status, body, headers = tool(base, "set_autonomy", {"level": "observe"})
    assert status == 401 and body["error"]["code"] == "unauthorized"
    assert headers["WWW-Authenticate"] == "Bearer"


@pytest.mark.parametrize("bad", ["wrong-token-0123456789", TOKEN + "x", TOKEN[:-1], ""])
def test_privileged_tool_with_a_wrong_token_is_401(live, bad):
    _, base = live
    assert tool(base, "set_autonomy", {"level": "observe"}, headers={"Authorization": f"Bearer {bad}"})[0] == 401
    assert tool(base, "set_autonomy", {"level": "observe"}, headers={"Authorization": f"Basic {TOKEN}"})[0] == 401


def test_privileged_tool_with_the_token_works(live):
    svc, base = live
    status, body, _ = tool(base, "set_autonomy", {"level": "observe"}, token=TOKEN)
    assert status == 200 and body["result"] == {"autonomy": "observe", "ceiling": "suggest"}
    assert svc.state.brain.autonomy == "observe"


def test_an_agent_cannot_raise_autonomy_past_the_ceiling(live):
    _, base = live
    status, body, _ = tool(base, "set_autonomy", {"level": "act"}, token=TOKEN)
    assert status == 403 and "ceiling" in body["error"]["message"]


def test_with_no_token_configured_privileged_tools_are_off(tmp_path):
    svc, base = start(tmp_path, agent_token=None)
    try:
        status, body, _ = tool(base, "set_autonomy", {"level": "observe"}, token="anything-at-all-123")
        assert status == 403 and "LISTENER55_AGENT_TOKEN" in body["error"]["message"]
        assert tool(base, "fleet_status")[0] == 200
        assert http(base, "/agent")[1]["auth"]["privileged_tools"].startswith("disabled")
    finally:
        svc.stop()


def test_arguments_are_validated_against_the_schema(live):
    _, base = live
    assert tool(base, "list_incidents", {"status": "sideways"})[0] == 422
    assert tool(base, "list_incidents", {"limit": 0})[0] == 422
    assert tool(base, "list_incidents", {"surprise": 1})[0] == 422
    assert tool(base, "ingest_event", {})[0] == 422  # required argument missing
    assert tool(base, "fleet_status", [1, 2])[0] == 422


def test_unknown_tool_is_404_and_bad_json_is_400(live):
    _, base = live
    assert tool(base, "launch_missiles")[0] == 404
    assert http(base, "/agent/tools/fleet_status", b"{nope")[0] == 400


def test_single_endpoint_call_accepts_function_calling_shapes(live):
    _, base = live
    # neutral
    assert http(base, "/agent/call", {"tool": "insights", "arguments": {}})[1]["ok"]
    # OpenAI tool_call: arguments arrive as a JSON *string*
    status, body, _ = http(
        base,
        "/agent/call",
        {"type": "function", "function": {"name": "list_incidents", "arguments": '{"status": "all"}'}},
    )
    assert status == 200 and body["result"]["count"] == 0
    # Anthropic tool_use: name + input
    assert http(base, "/agent/call", {"name": "fleet_status", "input": {"only_attention": True}})[0] == 200
    assert http(base, "/agent/call", {"name": "fleet_status", "arguments": "{bad"})[0] == 422
    assert http(base, "/agent/call", [1])[0] == 422


def test_an_agent_can_work_an_incident_end_to_end(live):
    svc, base = live
    inc = make_stale(svc)

    _, listed, _ = tool(base, "list_incidents")
    assert listed["result"]["incidents"][0]["id"] == inc["id"]
    assert listed["result"]["incidents"][0]["recommended_playbooks"][0]["playbook"] == "restart"

    _, briefing, _ = tool(base, "diagnose", {"incident_id": inc["id"]})
    assert briefing["result"]["state"] == "stale"
    assert briefing["result"]["hypotheses"]

    status, dry, _ = tool(base, "run_playbook", {"incident_id": inc["id"], "playbook": "restart", "dry_run": True}, token=TOKEN)
    assert status == 200 and dry["result"]["dry_run"] and dry["result"]["argv"] == ["true", "alpha"]

    status, ran, _ = tool(
        base,
        "run_playbook",
        {"incident_id": inc["id"], "playbook": "restart"},
        token=TOKEN,
        headers={"X-Agent-Name": "triage-bot"},
    )
    assert status == 200 and ran["result"]["ok"] and ran["result"]["actor"] == "triage-bot"

    assert tool(base, "add_incident_note", {"incident_id": inc["id"], "note": "restarted by hand"}, token=TOKEN)[0] == 200
    status, fb, _ = tool(base, "incident_feedback", {"incident_id": inc["id"], "verdict": "real"}, token=TOKEN)
    assert status == 200 and fb["result"]["verdict"] == "real"

    found = svc.state.brain.incidents.find(inc["id"])
    assert [a["dry_run"] for a in found["actions"]] == [True, False]
    assert found["notes"][0]["actor"] == "anonymous"


def test_run_playbook_refusals_are_409_and_unknown_incident_is_404(live):
    svc, base = live
    inc = make_stale(svc)
    status, body, _ = tool(base, "run_playbook", {"incident_id": inc["id"], "playbook": "nope"}, token=TOKEN)
    assert status == 409 and body["error"]["code"] == "refused"
    assert tool(base, "run_playbook", {"incident_id": "inc-999999", "playbook": "restart"}, token=TOKEN)[0] == 404
    assert tool(base, "diagnose", {"service": "nobody"})[0] == 404
    assert tool(base, "diagnose", {})[0] == 422


def test_silence_and_unsilence(live):
    svc, base = live
    args = {"service": "alpha", "minutes": 30, "reason": "deploy"}
    assert tool(base, "silence_service", args)[0] == 401
    assert tool(base, "silence_service", args, token=TOKEN)[0] == 200
    assert svc.state.brain.silenced("alpha")
    assert make_stale(svc) is None
    _, body, _ = tool(base, "unsilence_service", {"service": "alpha"}, token=TOKEN)
    assert body["result"]["was_silenced"] is True


def test_submit_self_report_and_forget_service(live):
    svc, base = live
    status, body, _ = tool(base, "submit_self_report", {"report": report("temp-thing")})
    assert status == 200 and body["result"]["id"] == "temp-thing"
    assert tool(base, "submit_self_report", {"report": {"service": "x"}})[0] == 422
    _, gone, _ = tool(base, "forget_service", {"service": "temp-thing"}, token=TOKEN)
    assert gone["result"] == {"service": "temp-thing", "baseline_removed": True, "report_removed": True}
    assert svc.state.fleet.one("temp-thing") is None


def test_reload_config_and_list_playbooks(live):
    _, base = live
    _, body, _ = tool(base, "reload_config", token=TOKEN)
    assert body["result"]["registry_reloaded"] is True and body["result"]["playbooks_ok"] is True
    _, books, _ = tool(base, "list_playbooks")
    assert [b["name"] for b in books["result"]["playbooks"]] == ["restart"]
    assert "argv" not in json.dumps(books["result"])  # commands are the operator's business


def test_every_call_is_audited_and_writes_reach_disk(live, tmp_path):
    _, base = live
    tool(base, "fleet_status", headers={"X-Agent-Name": "reader"})
    tool(base, "set_autonomy", {"level": "observe"}, token=TOKEN, headers={"X-Agent-Name": "writer"})
    tool(base, "set_autonomy", {"level": "observe"})
    _, body, _ = tool(base, "audit_log")
    entries = body["result"]["entries"]
    assert [(e["actor"], e["tool"], e["ok"]) for e in entries] == [
        ("reader", "fleet_status", True),
        ("writer", "set_autonomy", True),
        ("anonymous", "set_autonomy", False),
    ]
    on_disk = [json.loads(line) for line in (tmp_path / "state" / "audit.jsonl").read_text().splitlines()]
    assert [(e["tool"], e["ok"]) for e in on_disk] == [("set_autonomy", True), ("set_autonomy", False)]
    assert TOKEN not in (tmp_path / "state" / "audit.jsonl").read_text()


# ----------------------------------------------------------------- MCP/HTTP


def test_mcp_initialize_negotiates_the_protocol_version(live):
    _, base = live
    for wanted in MCP_VERSIONS:
        _, body, _ = rpc(base, "initialize", {"protocolVersion": wanted, "capabilities": {}, "clientInfo": {"name": "t", "version": "0"}})
        assert body["result"]["protocolVersion"] == wanted
    _, body, _ = rpc(base, "initialize", {"protocolVersion": "1999-01-01", "capabilities": {}})
    result = body["result"]
    assert result["protocolVersion"] == MCP_VERSIONS[0]
    assert result["serverInfo"]["name"] == "listener-5.5"
    assert "tools" in result["capabilities"]
    assert "fleet_status" in result["instructions"]


def test_mcp_lists_and_calls_tools(live):
    svc, base = live
    _, listed, _ = rpc(base, "tools/list")
    tools = {t["name"]: t for t in listed["result"]["tools"]}
    assert set(tools) == set(svc.state.agent.tools)
    assert tools["fleet_status"]["annotations"]["readOnlyHint"] is True
    assert tools["run_playbook"]["annotations"]["readOnlyHint"] is False

    _, called, _ = rpc(base, "tools/call", {"name": "fleet_status", "arguments": {}})
    result = called["result"]
    assert result["isError"] is False
    assert json.loads(result["content"][0]["text"]) == result["structuredContent"]
    assert result["structuredContent"]["registry"]["loaded"] is True


def test_mcp_tool_failures_are_results_not_protocol_errors(live):
    _, base = live
    _, body, _ = rpc(base, "tools/call", {"name": "set_autonomy", "arguments": {"level": "observe"}})
    assert body["result"]["isError"] is True
    assert "unauthorized" in body["result"]["content"][0]["text"]
    _, body, _ = rpc(base, "tools/call", {"name": "set_autonomy", "arguments": {"level": "observe"}}, token=TOKEN)
    assert body["result"]["isError"] is False
    _, body, _ = rpc(base, "tools/call", {"name": "no_such_tool", "arguments": {}})
    assert body["error"]["code"] == -32602


def test_mcp_protocol_edges(live):
    _, base = live
    assert rpc(base, "ping")[1]["result"] == {}
    assert rpc(base, "does/not/exist")[1]["error"]["code"] == -32601
    # notifications are accepted and never answered
    status, body, _ = http(base, "/mcp", {"jsonrpc": "2.0", "method": "notifications/initialized"})
    assert status == 202 and body is None
    # batch: one request, one notification -> one response
    status, body, _ = http(
        base,
        "/mcp",
        [{"jsonrpc": "2.0", "id": "a", "method": "ping"}, {"jsonrpc": "2.0", "method": "notifications/x"}],
    )
    assert status == 200 and [r["id"] for r in body] == ["a"]
    assert http(base, "/mcp", b"{nope")[1]["error"]["code"] == -32700
    assert http(base, "/mcp", {"id": 1, "method": "ping"})[1]["error"]["code"] == -32600
    status, _, headers = http(base, "/mcp")
    assert status == 405 and headers["Allow"] == "POST"


def test_mcp_resources_and_prompts(live):
    _, base = live
    uris = [r["uri"] for r in rpc(base, "resources/list")[1]["result"]["resources"]]
    assert "listener55://fleet" in uris
    for uri in uris:
        contents = rpc(base, "resources/read", {"uri": uri})[1]["result"]["contents"]
        assert contents[0]["uri"] == uri and contents[0]["text"]
    assert rpc(base, "resources/read", {"uri": "listener55://nope"})[1]["error"]["code"] == -32002
    assert rpc(base, "prompts/list")[1]["result"]["prompts"][0]["name"] == "triage"
    assert rpc(base, "prompts/get", {"name": "triage"})[1]["result"]["messages"][0]["role"] == "user"


# ---------------------------------------------------------------- MCP/stdio


def bridge(base, lines, token=None) -> list[dict]:
    out = io.StringIO()
    assert run_stdio_bridge(base, token, io.StringIO("\n".join(lines) + "\n"), out) == 0
    return [json.loads(line) for line in out.getvalue().splitlines()]


def test_stdio_bridge_relays_to_the_running_listener(live):
    _, base = live
    replies = bridge(
        base,
        [
            json.dumps({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": MCP_VERSIONS[0]}}),
            json.dumps({"jsonrpc": "2.0", "method": "notifications/initialized"}),
            "",
            json.dumps({"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": {"name": "set_autonomy", "arguments": {"level": "observe"}}}),
            "not json at all",
        ],
        token=TOKEN,
    )
    assert [r.get("id") for r in replies] == [1, 2, None]
    assert replies[0]["result"]["serverInfo"]["version"]
    assert replies[1]["result"]["isError"] is False  # the bridge forwarded the token
    assert replies[2]["error"]["code"] == -32700
    assert all("\n" not in json.dumps(r) for r in replies)


def test_stdio_bridge_reports_an_unreachable_listener():
    replies = bridge(
        f"http://127.0.0.1:{free_port()}",
        [json.dumps({"jsonrpc": "2.0", "id": 9, "method": "ping"}), json.dumps({"jsonrpc": "2.0", "method": "notifications/x"})],
    )
    assert len(replies) == 1 and replies[0]["id"] == 9
    assert "cannot reach listener" in replies[0]["error"]["message"]


# ---------------------------------------------------------------- hardening


def test_cross_site_browser_requests_are_refused(live):
    _, base = live
    evil = {"Origin": "https://evil.example"}
    assert tool(base, "fleet_status", headers=evil)[0] == 403
    assert rpc(base, "ping", headers=evil)[0] == 403
    assert http(base, "/agent/call", {"tool": "insights"}, headers=evil)[0] == 403
    assert tool(base, "fleet_status", headers={"Origin": "http://localhost:3000"})[0] == 200
    assert tool(base, "fleet_status", headers={"Origin": "null"})[0] == 403


def test_allowed_origins_are_configurable(tmp_path):
    svc, base = start(tmp_path, allowed_origins=("https://console.example",))
    try:
        assert tool(base, "fleet_status", headers={"Origin": "https://console.example"})[0] == 200
        assert tool(base, "fleet_status", headers={"Origin": "https://other.example"})[0] == 403
    finally:
        svc.stop()


def test_a_crashing_handler_is_a_500_and_the_server_keeps_serving(live):
    svc, base = live
    svc.state.brain.insights = lambda: 1 / 0
    status, body, _ = http(base, "/insights")
    assert status == 500 and body["detail"] == "ZeroDivisionError"
    status, body, _ = tool(base, "insights")
    assert status == 500 and body["error"]["code"] == "internal"
    assert http(base, "/healthz")[0] == 200
    assert svc.metrics.internal_errors == 2


def test_idempotency_key_makes_a_retry_safe(live):
    svc, base = live
    key = {"Idempotency-Key": "evt-1"}
    assert http(base, "/events", {"type": "ping"}, headers=key)[0] == 202
    status, body, _ = http(base, "/events", {"type": "ping"}, headers=key)
    assert status == 200 and body == {"ok": True, "duplicate": True}
    assert http(base, "/events", {"type": "ping"})[0] == 202
    counts = svc.metrics.snapshot_counts()
    assert counts["received"] == 2 and svc.metrics.deduplicated == 1


def test_a_failed_event_can_be_retried_under_the_same_key(live):
    svc, base = live
    svc.state._process = lambda item: {"ok": False, "error": "nope"}
    assert http(base, "/events", {"a": 1}, headers={"Idempotency-Key": "k"})[0] == 422
    del svc.state._process
    assert http(base, "/events", {"a": 1}, headers={"Idempotency-Key": "k"})[0] == 202


def test_failed_events_are_dead_lettered_and_can_be_replayed(live):
    svc, base = live
    real = svc.state._process
    svc.state._process = lambda item: (svc.metrics.mark_received(), svc.metrics.mark_error("downstream"), {"ok": False, "error": "downstream"})[2]
    assert http(base, "/events", {"type": "order", "n": 1})[0] == 422
    svc.state._process = real

    _, body, _ = tool(base, "recent_events", {"failed_only": True})
    assert body["result"]["dead_letter_count"] == 1
    assert body["result"]["events"][0]["event_type"] == "order"
    assert json.loads(body["result"]["dead_letters"][0]["item"]) == {"type": "order", "n": 1}

    assert tool(base, "replay_dead_letters")[0] == 401
    _, body, _ = tool(base, "replay_dead_letters", token=TOKEN)
    assert body["result"] == {"replayed": 1, "succeeded": 1, "remaining": 0}


def test_liveness_readiness_and_prometheus(live):
    svc, base = live
    assert http(base, "/livez")[1] == {"ok": True}
    assert http(base, "/readyz")[1] == {"ready": True, "status": "healthy"}
    svc.metrics.set_status("stopping")
    assert http(base, "/readyz")[0] == 503
    svc.metrics.set_status("healthy")
    status, text, headers = http(base, "/metrics/prometheus")
    assert status == 200 and headers["Content-Type"].startswith("text/plain")
    assert "listener55_up 1" in text
    assert 'listener55_fleet_services{state="unknown"} 1' in text
    for line in text.splitlines():
        assert line.startswith("#") or len(line.split(" ")) == 2, line


def test_incidents_and_insights_have_plain_get_routes(live):
    svc, base = live
    inc = make_stale(svc)
    _, body, _ = http(base, "/incidents")
    assert body["incidents"][0]["id"] == inc["id"]
    assert http(base, "/incidents?status=nope")[0] == 400
    assert http(base, "/incidents?status=resolved")[1]["incidents"] == []
    assert "services" in http(base, "/insights")[1]


def test_healthz_stays_contract_shaped_and_carries_the_new_meta(live):
    from listener55.fleet import ORG_SELF_REPORT_SCHEMA
    from listener55.schema import validate_payload

    _, base = live
    _, body, _ = http(base, "/healthz")
    assert validate_payload(body, ORG_SELF_REPORT_SCHEMA) == []
    assert body["meta"]["autonomy"] == "suggest"
    # /metrics remains valid against this service's own strict schema
    assert validate_payload(http(base, "/metrics")[1]) == []


def test_emitted_self_reports_still_satisfy_the_strict_schema(live, tmp_path):
    from listener55.schema import validate_payload

    svc, _ = live
    svc.state.reporter.emit_once()
    line = json.loads((tmp_path / "sr.jsonl").read_text().splitlines()[-1])
    assert validate_payload(line) == []
    assert line["meta"]["version"]


def test_learned_state_survives_a_service_restart(tmp_path):
    svc, base = start(tmp_path)
    inc = make_stale(svc)
    tool(base, "add_incident_note", {"incident_id": inc["id"], "note": "remember me"}, token=TOKEN)
    svc.stop()

    again, base = start(tmp_path)
    try:
        _, body, _ = tool(base, "list_incidents")
        assert body["result"]["incidents"][0]["notes"][0]["note"] == "remember me"
    finally:
        again.stop()


# --------------------------------------------------------------------- CLI


def run_cli(args, env_extra, **kw):
    env = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1] / "src"), **env_extra)
    return subprocess.run([sys.executable, "-m", "listener55.cli", *args], env=env, capture_output=True, text=True, timeout=20, **kw)


def test_cli_prints_tool_catalogs_without_touching_state(tmp_path):
    env = {"LISTENER55_STATE_DIR": str(tmp_path / "st"), "LISTENER55_REPORT_LOG": str(tmp_path / "r.jsonl")}
    out = run_cli(["tools", "--format", "openai"], env)
    assert out.returncode == 0, out.stderr
    tools = json.loads(out.stdout)
    assert tools[0]["type"] == "function" and len(tools) >= 15
    assert not (tmp_path / "st").exists()


def test_cli_mcp_bridge_end_to_end(live):
    _, base = live
    msg = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "tools/list"}) + "\n"
    out = run_cli(["mcp", "--url", base], {"LISTENER55_REPORT_LOG": "/dev/null"}, input=msg)
    assert out.returncode == 0, out.stderr
    assert len(json.loads(out.stdout)["result"]["tools"]) >= 15


def test_sigterm_stops_the_service_promptly(tmp_path):
    """Regression: the signal handler used to call httpd.shutdown() on the
    thread running serve_forever, which waits on itself. The service ignored
    SIGTERM until it was killed."""
    port = free_port()
    env = dict(
        os.environ,
        PYTHONPATH=str(Path(__file__).resolve().parents[1] / "src"),
        LISTENER55_PORT=str(port),
        LISTENER55_REPORT_LOG=str(tmp_path / "r.jsonl"),
        LISTENER55_STATE_DIR=str(tmp_path / "st"),
    )
    proc = subprocess.Popen([sys.executable, "-m", "listener55.cli"], env=env, stderr=subprocess.DEVNULL)
    try:
        deadline = time.time() + 5
        while time.time() < deadline:
            try:
                urllib.request.urlopen(f"http://127.0.0.1:{port}/livez", timeout=0.5).close()
                break
            except Exception:
                time.sleep(0.05)
        else:
            pytest.fail("service did not start")
        proc.send_signal(signal.SIGTERM)
        assert proc.wait(timeout=8) == 0
    finally:
        if proc.poll() is None:
            proc.kill()
    last = json.loads((tmp_path / "r.jsonl").read_text().splitlines()[-1])
    assert last["status"] == "stopping"  # the farewell report still goes out
    assert (tmp_path / "st" / "state.json").exists()  # and what was learned is saved

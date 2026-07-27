"""Unit tests for listener-5.5."""

from __future__ import annotations

import json
import os
import socket
import tempfile
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

import pytest

from listener55.config import Config
from listener55.reporter import SelfReporter
from listener55.schema import (
    SELF_REPORT_SCHEMA,
    Metrics,
    assert_valid,
    load_schema_from_repo,
    validate_payload,
)
from listener55.server import ListenerService, ListenerState


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def test_config_from_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    log_path = tmp_path / "r.jsonl"
    monkeypatch.setenv("LISTENER55_HOST", "0.0.0.0")
    monkeypatch.setenv("LISTENER55_PORT", "9999")
    monkeypatch.setenv("LISTENER55_REPORT_INTERVAL", "12.5")
    monkeypatch.setenv("LISTENER55_REPORT_LOG", str(log_path))
    monkeypatch.setenv("LISTENER55_INSTANCE_ID", "test-instance")
    monkeypatch.delenv("LISTENER55_REPORT_URL", raising=False)
    cfg = Config.from_env()
    cfg.validate()
    assert cfg.host == "0.0.0.0"
    assert cfg.port == 9999
    assert cfg.report_interval_seconds == 12.5
    assert cfg.report_log_path == log_path
    assert cfg.instance_id == "test-instance"


def test_config_rejects_bad_port(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LISTENER55_PORT", "70000")
    monkeypatch.setenv("LISTENER55_REPORT_LOG", "/tmp/x.jsonl")
    cfg = Config.from_env()
    with pytest.raises(ValueError, match="port"):
        cfg.validate()


def test_self_report_payload_validates() -> None:
    m = Metrics()
    m.set_status("healthy")
    m.mark_received()
    m.mark_ok()
    payload = m.build_payload(instance_id="i1", host="127.0.0.1", port=8555)
    errors = validate_payload(payload)
    assert errors == [], errors
    assert_valid(payload)


def test_schema_file_matches_embedded() -> None:
    file_schema = load_schema_from_repo()
    # Compare structural identity of required keys / const values that matter.
    assert file_schema["required"] == SELF_REPORT_SCHEMA["required"]
    assert file_schema["properties"]["service"]["const"] == "listener-5.5"
    assert file_schema["properties"]["schema_version"]["const"] == "1.0"


def test_invalid_payload_caught() -> None:
    bad = {
        "schema_version": "9.9",
        "service": "nope",
        "instance_id": "",
        "status": "unknown",
        "uptime_seconds": -1,
        "started_at": "x",
        "reported_at": "y",
        "listen": {"host": "h"},
        "counts": {},
        "last_error": 123,
    }
    errs = validate_payload(bad)
    assert errs, "expected validation errors"


def test_process_item_ok_and_error() -> None:
    cfg = Config(
        host="127.0.0.1",
        port=1,
        report_url=None,
        report_interval_seconds=30,
        report_log_path=Path("/tmp/unused.jsonl"),
        instance_id="t",
        max_body_bytes=1024,
        process_fail_rate=0.0,
    )
    st = ListenerState(cfg)
    ok = st.process_item({"event": "hello"})
    assert ok["ok"] is True
    bad = st.process_item(None)
    assert bad["ok"] is False
    counts = st.metrics.snapshot_counts()
    assert counts["received"] == 2
    assert counts["processed_ok"] == 1
    assert counts["processed_error"] == 1


def test_reporter_writes_jsonl(tmp_path: Path) -> None:
    path = tmp_path / "reports.jsonl"
    m = Metrics()
    m.set_status("healthy")
    r = SelfReporter(
        metrics=m,
        instance_id="abc",
        host="127.0.0.1",
        port=8555,
        interval_seconds=60,
        report_log_path=path,
    )
    payload = r.emit_once()
    assert path.exists()
    line = path.read_text(encoding="utf-8").strip()
    loaded = json.loads(line)
    assert loaded == payload
    assert validate_payload(loaded) == []
    assert m.snapshot_counts()["self_reports_sent"] == 1


def test_reporter_posts_http(tmp_path: Path) -> None:
    m = Metrics()
    m.set_status("healthy")
    seen: list[tuple[str, bytes]] = []

    def fake_post(url: str, body: bytes, timeout: float) -> tuple[int, str]:
        seen.append((url, body))
        return 204, ""

    r = SelfReporter(
        metrics=m,
        instance_id="abc",
        host="127.0.0.1",
        port=8555,
        interval_seconds=60,
        report_url="http://monitor.example/ingest",
        report_log_path=tmp_path / "also.jsonl",
        post_fn=fake_post,
    )
    payload = r.emit_once()
    assert len(seen) == 1
    assert seen[0][0] == "http://monitor.example/ingest"
    assert json.loads(seen[0][1].decode()) == payload
    assert validate_payload(payload) == []


def test_http_server_ingest_and_health(tmp_path: Path) -> None:
    port = free_port()
    cfg = Config(
        host="127.0.0.1",
        port=port,
        report_url=None,
        report_interval_seconds=3600,
        report_log_path=tmp_path / "sr.jsonl",
        instance_id="live-test",
        max_body_bytes=65536,
        process_fail_rate=0.0,
    )
    svc = ListenerService(cfg)
    svc.start(blocking=False)
    try:
        # Wait briefly for bind.
        deadline = time.time() + 3
        while time.time() < deadline:
            try:
                with urllib.request.urlopen(f"http://127.0.0.1:{port}/healthz", timeout=0.5) as resp:
                    health = json.loads(resp.read().decode())
                    break
            except Exception:
                time.sleep(0.05)
        else:
            pytest.fail("server did not become ready")

        assert health["service"] == "listener-5.5"
        assert health["instance_id"] == "live-test"

        req = urllib.request.Request(
            f"http://127.0.0.1:{port}/events",
            data=json.dumps({"type": "ping", "n": 1}).encode(),
            method="POST",
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=2) as resp:
            assert resp.status == 202
            body = json.loads(resp.read().decode())
            assert body["ok"] is True

        with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=2) as resp:
            metrics = json.loads(resp.read().decode())
        assert validate_payload(metrics) == []
        assert metrics["counts"]["received"] >= 1
        assert metrics["counts"]["processed_ok"] >= 1
    finally:
        svc.stop()


def test_cli_once_report(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.setenv("LISTENER55_REPORT_LOG", str(tmp_path / "one.jsonl"))
    monkeypatch.setenv("LISTENER55_INSTANCE_ID", "cli-once")
    monkeypatch.setenv("LISTENER55_PORT", "8555")
    from listener55.cli import main

    rc = main(["--once-report"])
    assert rc == 0
    out = capsys.readouterr().out
    # print(dict) uses single quotes — parse carefully via jsonl file instead
    lines = (tmp_path / "one.jsonl").read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1
    payload = json.loads(lines[0])
    assert payload["instance_id"] == "cli-once"
    assert validate_payload(payload) == []

"""Tests for the fleet bus: ingest, roster join, staleness, HTTP routes."""

from __future__ import annotations

import json
import socket
import threading
import time
import urllib.error
import urllib.request

import pytest

from listener55.config import Config
from listener55.fleet import (
    DEFAULT_STALE_AFTER,
    STATE_DEGRADED,
    STATE_HEALTHY,
    STATE_STALE,
    STATE_UNKNOWN,
    FleetStore,
    Roster,
    resolve_registry_path,
)
from listener55.server import ListenerService


def report(
    registry_id="alpha",
    service="Alpha Service",
    status="healthy",
    **overrides,
):
    body = {
        "schema_version": "1.0",
        "service": service,
        "registry_id": registry_id,
        "instance_id": "i-1",
        "status": status,
        "uptime_seconds": 12.5,
        "started_at": "2026-09-20T05:00:00.000Z",
        "reported_at": "2026-09-20T05:00:12.500Z",
        "listen": {"host": "127.0.0.1", "port": 9001},
        "counts": {"received": 3},
        "last_error": None,
    }
    body.update(overrides)
    return body


REGISTRY = {
    "schema_version": "1.0",
    "tailnet": {"ip": "10.1.2.3"},
    "services": [
        {
            "id": "alpha",
            "name": "Alpha Service",
            "cluster": "viewer",
            "status": "live",
            "bind": "tailnet",
            "port": 9001,
            "health_path": "/healthz",
            "self_report": True,
            "repo": "org/alpha",
            "owner": "team",
        },
        {
            "id": "beta",
            "name": "Beta Service",
            "cluster": "viewer",
            "status": "live",
            "bind": "loopback",
            "port": 9002,
            "health_path": "/healthz",
            "self_report": True,
            "repo": "org/beta",
            "owner": "team",
        },
        {
            "id": "ghost",
            "name": "Tombstoned Thing",
            "cluster": "unclaimed",
            "status": "tombstoned",
            "bind": "tailnet",
            "port": 9003,
            "health_path": "/health",
            "self_report": True,
            "owner": "unknown",
        },
        {
            "id": "quiet",
            "name": "No Self Report",
            "cluster": "platform",
            "status": "live",
            "bind": "tailnet",
            "port": 9004,
            "health_path": "/health",
            "self_report": False,
            "owner": "team",
        },
    ],
}


@pytest.fixture
def registry_file(tmp_path):
    path = tmp_path / "service-registry.json"
    path.write_text(json.dumps(REGISTRY), encoding="utf-8")
    return path


@pytest.fixture
def store(registry_file):
    return FleetStore(roster=Roster(registry_file))


# ------------------------------------------------------------------ roster


def test_roster_loads_services(registry_file):
    roster = Roster(registry_file)
    assert roster.loaded
    assert set(roster.services) == {"alpha", "beta", "ghost", "quiet"}
    assert roster.tailnet["ip"] == "10.1.2.3"


def test_roster_expects_only_live_self_reporting_services(registry_file):
    """A tombstoned row must not raise a permanent `unknown` alarm."""
    assert sorted(Roster(registry_file).expected_ids()) == ["alpha", "beta"]


def test_missing_registry_is_not_fatal(tmp_path):
    roster = Roster(tmp_path / "nope.json")
    assert not roster.loaded
    assert roster.load_error
    assert roster.describe()["loaded"] is False


def test_malformed_registry_reports_the_reason(tmp_path):
    path = tmp_path / "bad.json"
    path.write_text("{not json", encoding="utf-8")
    roster = Roster(path)
    assert not roster.loaded
    assert "JSONDecodeError" in roster.load_error


def test_unsupported_registry_version_is_refused(tmp_path):
    path = tmp_path / "v9.json"
    path.write_text(json.dumps({"schema_version": "9.0", "services": []}), encoding="utf-8")
    roster = Roster(path)
    assert not roster.loaded
    assert "schema_version" in roster.load_error


def test_resolve_registry_path_prefers_explicit(registry_file):
    assert resolve_registry_path(str(registry_file)) == registry_file


def test_resolve_registry_path_returns_none_when_absent(tmp_path, monkeypatch):
    monkeypatch.delenv("LISTENER55_REGISTRY", raising=False)
    monkeypatch.chdir(tmp_path)
    assert resolve_registry_path() is None


def test_resolve_registry_path_reads_the_env_var(registry_file, monkeypatch):
    monkeypatch.setenv("LISTENER55_REGISTRY", str(registry_file))
    assert resolve_registry_path() == registry_file


# ------------------------------------------------------------------ ingest


def test_ingest_accepts_a_valid_report(store):
    accepted, errors = store.ingest(report())
    assert accepted and errors == []


def test_ingest_rejects_a_non_object(store):
    accepted, errors = store.ingest(["not", "an", "object"])
    assert not accepted
    assert "JSON object" in errors[0]


def test_ingest_rejects_a_report_missing_required_fields(store):
    bad = report()
    del bad["status"]
    accepted, errors = store.ingest(bad)
    assert not accepted
    assert any("status" in e for e in errors)


def test_ingest_rejects_a_bad_status_value(store):
    accepted, errors = store.ingest(report(status="vibing"))
    assert not accepted
    assert errors


def test_ingest_rejects_a_wrong_schema_version(store):
    accepted, errors = store.ingest(report(schema_version="2.0"))
    assert not accepted


def test_rejected_reports_are_not_stored(store):
    store.ingest(report(status="vibing"))
    assert store.one("alpha")["state"] == STATE_UNKNOWN


def test_ingest_requires_an_identity(store):
    anon = report()
    del anon["registry_id"]
    del anon["service"]
    accepted, errors = store.ingest(anon)
    assert not accepted


def test_key_falls_back_to_service_name_without_registry_id(store):
    body = report()
    del body["registry_id"]
    assert store.key_for(body) == "Alpha Service"
    assert store.ingest(body)[0]


def test_ingest_counts_repeat_reports(store):
    for _ in range(3):
        store.ingest(report())
    assert store.one("alpha")["reports_received"] == 3


def test_later_report_replaces_the_earlier_one(store):
    store.ingest(report(status="healthy"))
    store.ingest(report(status="degraded", degraded_reasons=["disk full"]))
    row = store.one("alpha")
    assert row["state"] == STATE_DEGRADED
    assert row["degraded_reasons"] == ["disk full"]


def test_counts_may_carry_arbitrary_keys(store):
    assert store.ingest(report(counts={"widgets_frobbed": 9}))[0]


def test_negative_counts_are_refused(store):
    accepted, _ = store.ingest(report(counts={"received": -1}))
    assert not accepted


# ----------------------------------------------------------------- rollup


def test_rollup_lists_expected_services_that_never_reported(store):
    roll = store.rollup()
    by_id = {r["id"]: r for r in roll["services"]}
    assert by_id["beta"]["state"] == STATE_UNKNOWN
    assert by_id["beta"]["reports_received"] == 0
    assert by_id["beta"]["expected"] is True


def test_rollup_is_degraded_while_an_expected_service_is_silent(store):
    store.ingest(report())  # alpha healthy, beta still unknown
    roll = store.rollup()
    assert roll["status"] == "degraded"
    assert roll["needs_attention"] == ["beta"]


def test_rollup_is_ok_once_every_expected_service_reports(store):
    store.ingest(report(registry_id="alpha"))
    store.ingest(report(registry_id="beta", service="Beta Service"))
    roll = store.rollup()
    assert roll["status"] == "ok"
    assert roll["needs_attention"] == []
    assert roll["summary"][STATE_HEALTHY] == 2


def test_rollup_enriches_rows_from_the_registry(store):
    store.ingest(report())
    row = {r["id"]: r for r in store.rollup()["services"]}["alpha"]
    assert row["name"] == "Alpha Service"
    assert row["cluster"] == "viewer"
    assert row["port"] == 9001
    assert row["repo"] == "org/alpha"
    assert row["registered"] is True


def test_unregistered_reporter_is_surfaced_not_hidden(store):
    store.ingest(report(registry_id="stranger", service="Who Dis"))
    row = {r["id"]: r for r in store.rollup()["services"]}["stranger"]
    assert row["registered"] is False
    assert row["expected"] is False
    assert row["state"] == STATE_HEALTHY


def test_unregistered_reporter_does_not_degrade_the_fleet(store):
    store.ingest(report(registry_id="alpha"))
    store.ingest(report(registry_id="beta", service="Beta"))
    store.ingest(report(registry_id="stranger", status="degraded"))
    assert store.rollup()["status"] == "ok"


def test_tombstoned_service_is_absent_until_it_reports(store):
    assert "ghost" not in {r["id"] for r in store.rollup()["services"]}


def test_service_that_does_not_self_report_is_not_expected(store):
    assert "quiet" not in {r["id"] for r in store.rollup()["services"]}


def test_stale_report_is_flagged(registry_file):
    store = FleetStore(roster=Roster(registry_file), stale_after=0.05)
    store.ingest(report())
    time.sleep(0.08)
    row = store.one("alpha")
    assert row["state"] == STATE_STALE
    assert row["age_seconds"] > 0.05


def test_stale_counts_as_needing_attention(registry_file):
    store = FleetStore(roster=Roster(registry_file), stale_after=0.05)
    store.ingest(report(registry_id="alpha"))
    store.ingest(report(registry_id="beta", service="Beta"))
    time.sleep(0.08)
    roll = store.rollup()
    assert roll["status"] == "degraded"
    assert sorted(roll["needs_attention"]) == ["alpha", "beta"]


def test_stopping_status_is_treated_as_degraded(store):
    store.ingest(report(status="stopping"))
    assert store.one("alpha")["state"] == STATE_DEGRADED


def test_rollup_reports_registry_provenance(store, registry_file):
    reg = store.rollup()["registry"]
    assert reg["loaded"] is True
    assert reg["path"] == str(registry_file)
    assert reg["expected_reporters"] == 2


def test_rollup_without_a_registry_still_serves_reports(tmp_path):
    store = FleetStore(roster=Roster(tmp_path / "absent.json"))
    store.ingest(report())
    roll = store.rollup()
    assert roll["registry"]["loaded"] is False
    assert roll["status"] == "ok"  # nothing is *expected*, so nothing is missing
    assert {r["id"] for r in roll["services"]} == {"alpha"}


def test_one_returns_none_for_a_total_stranger(store):
    assert store.one("nobody") is None


def test_one_returns_a_row_for_a_registered_silent_service(store):
    row = store.one("beta")
    assert row is not None
    assert row["state"] == STATE_UNKNOWN


def test_default_stale_window_allows_three_missed_pushes():
    """The contract's default push interval is 30s."""
    assert DEFAULT_STALE_AFTER == pytest.approx(90.0)


def test_ingest_is_thread_safe(store):
    def push(n):
        for i in range(25):
            store.ingest(report(registry_id=f"svc-{n}", service=f"S{n}"))

    threads = [threading.Thread(target=push, args=(n,)) for n in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    rows = {r["id"]: r for r in store.rollup()["services"]}
    for n in range(6):
        assert rows[f"svc-{n}"]["reports_received"] == 25


# ------------------------------------------------------------------- HTTP


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _get(url):
    with urllib.request.urlopen(url, timeout=5) as resp:
        return resp.status, json.loads(resp.read().decode())


def _post(url, payload):
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode())


@pytest.fixture
def service(tmp_path, registry_file):
    port = _free_port()
    cfg = Config(
        host="127.0.0.1",
        port=port,
        report_url=None,
        report_interval_seconds=3600,
        report_log_path=tmp_path / "sr.jsonl",
        instance_id="fleet-test",
        max_body_bytes=65536,
        process_fail_rate=0.0,
        registry_id="alpha",
        registry_path=str(registry_file),
    )
    svc = ListenerService(cfg)
    svc.start(blocking=False)
    time.sleep(0.15)
    yield f"http://127.0.0.1:{port}"
    svc.stop()


def test_healthz_is_contract_shaped(service):
    code, body = _get(f"{service}/healthz")
    assert code == 200
    for field in (
        "schema_version",
        "service",
        "registry_id",
        "instance_id",
        "status",
        "uptime_seconds",
        "started_at",
        "reported_at",
        "listen",
        "counts",
        "last_error",
    ):
        assert field in body, field
    assert body["schema_version"] == "1.0"


def test_self_report_endpoint_accepts_and_lists(service):
    code, body = _post(f"{service}/self-report", report(registry_id="beta", service="Beta"))
    assert code == 202
    assert body == {"ok": True, "id": "beta"}
    code, roll = _get(f"{service}/fleet")
    assert code == 200
    assert {r["id"] for r in roll["services"]} >= {"alpha", "beta"}


def test_self_report_endpoint_rejects_invalid_payloads(service):
    code, body = _post(f"{service}/self-report", {"nope": True})
    assert code == 422
    assert body["ok"] is False
    assert body["errors"]


def test_fleet_always_answers_200_even_when_degraded(service):
    """/fleet describes the fleet; it is not this service's own health."""
    code, roll = _get(f"{service}/fleet")
    assert code == 200
    assert roll["status"] == "degraded"  # beta has never reported
    assert "beta" in roll["needs_attention"]


def test_the_bus_appears_in_its_own_roster(service):
    _, roll = _get(f"{service}/fleet")
    alpha = {r["id"]: r for r in roll["services"]}["alpha"]
    assert alpha["state"] == STATE_HEALTHY
    assert alpha["reports_received"] >= 1


def test_fleet_single_service_route(service):
    code, row = _get(f"{service}/fleet/beta")
    assert code == 200
    assert row["id"] == "beta"
    assert row["state"] == STATE_UNKNOWN


def test_fleet_unknown_service_is_404(service):
    try:
        _get(f"{service}/fleet/nobody")
    except urllib.error.HTTPError as exc:
        assert exc.code == 404
        assert json.loads(exc.read().decode())["error"] == "unknown service"
    else:
        pytest.fail("expected 404")


def test_event_ingest_still_works(service):
    code, body = _post(f"{service}/events", {"type": "ping"})
    assert code == 202
    assert body["ok"] is True

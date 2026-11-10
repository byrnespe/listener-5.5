"""Tests for the 0.2 layers: durable state, learned baselines, incident
memory, self-healing primitives and the supervisor that ties them together."""

from __future__ import annotations

import json
import os
import socket
import time
from pathlib import Path

import pytest

from listener55 import schema as schema_mod
from listener55.brain import FLEET_SERVICE, Brain
from listener55.config import Config
from listener55.fleet import FleetStore, Roster
from listener55.heal import CircuitBreaker, Playbooks, sd_notify
from listener55.incidents import IncidentLog, fingerprint
from listener55.learn import DEFAULT_TOLERANCE, Baselines
from listener55.reporter import SelfReporter
from listener55.schema import Metrics
from listener55.store import StateStore


# ------------------------------------------------------------------ helpers


def report(registry_id="alpha", status="healthy", started="2026-09-20T05:00:00.000Z", **over):
    body = {
        "schema_version": "1.0",
        "service": registry_id,
        "registry_id": registry_id,
        "instance_id": "i-1",
        "status": status,
        "uptime_seconds": 12.5,
        "started_at": started,
        "reported_at": "2026-09-20T05:00:12.500Z",
        "counts": {"received": 3},
        "last_error": None,
    }
    body.update(over)
    return body


def registry(tmp_path: Path, ids=("alpha", "beta", "gamma", "delta")) -> Path:
    path = tmp_path / "service-registry.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": "1.0",
                "services": [
                    {"id": i, "name": i, "status": "live", "self_report": True, "port": 9000 + n}
                    for n, i in enumerate(ids)
                ],
            }
        ),
        encoding="utf-8",
    )
    return path


class Clock:
    def __init__(self, start: float = 1_000_000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def make_config(tmp_path: Path, **over) -> Config:
    base = dict(
        host="127.0.0.1",
        port=1,
        report_url=None,
        report_interval_seconds=30,
        report_log_path=tmp_path / "sr.jsonl",
        instance_id="t",
        max_body_bytes=1024,
        process_fail_rate=0.0,
    )
    base.update(over)
    return Config(**base)


def build(tmp_path: Path, *, ids=("alpha", "beta", "gamma", "delta"), playbooks=None, runner=None, **over):
    """A Brain over real components, with a controllable wall clock."""
    wall = Clock()
    config = make_config(tmp_path, **over)
    metrics = Metrics()
    metrics.set_status("healthy")
    fleet = FleetStore(roster=Roster(registry(tmp_path, ids)), stale_after=90.0)
    reporter = SelfReporter(
        metrics=metrics,
        instance_id="t",
        host="127.0.0.1",
        port=8555,
        interval_seconds=30,
        report_log_path=tmp_path / "sr.jsonl",
    )
    store = StateStore(config.state_dir)
    books = None
    if playbooks is not None:
        path = tmp_path / "playbooks.json"
        path.write_text(json.dumps({"playbooks": playbooks}), encoding="utf-8")
        books = Playbooks(
            str(path),
            store.section("playbook_outcomes"),
            store.touch,
            max_per_hour=config.max_actions_per_hour,
            runner=runner,
            wall=wall,
        )
    brain = Brain(
        config=config, metrics=metrics, fleet=fleet, reporter=reporter, store=store, playbooks=books, wall=wall
    )
    brain.grace_seconds = 0
    return brain, wall


def age(fleet: FleetStore, key: str, seconds: float) -> None:
    fleet._reports[key]["received_mono"] -= seconds


def all_report(brain: Brain, ids=("alpha", "beta", "gamma", "delta")) -> None:
    for i in ids:
        assert brain.fleet.ingest(report(i))[0]


def wait_for(predicate, timeout: float = 3.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return
        time.sleep(0.02)
    pytest.fail("condition not met in time")


RESTART = {
    "name": "restart",
    "match": {"service": "*", "kinds": ["stale"]},
    "action": {"type": "command", "argv": ["svc", "restart", "{service}"]},
    "cooldown_seconds": 0,
    "max_attempts": 2,
}


# -------------------------------------------------------------------- store


def test_state_round_trips(tmp_path):
    a = StateStore(tmp_path)
    a.section("things")["x"] = 1
    a.touch()
    assert a.save() is True
    assert StateStore(tmp_path).section("things") == {"x": 1}


def test_corrupt_state_is_quarantined_not_fatal(tmp_path):
    (tmp_path / "state.json").write_text("{ this is not json", encoding="utf-8")
    store = StateStore(tmp_path)
    assert store.section("things") == {}
    assert any(p.name.startswith("state.json.quarantined-") for p in tmp_path.iterdir())
    assert "starting fresh" in store.notes[0]


def test_state_from_a_newer_version_is_set_aside(tmp_path):
    (tmp_path / "state.json").write_text(
        json.dumps({"state_version": 99, "sections": {"a": {"b": 1}}}), encoding="utf-8"
    )
    store = StateStore(tmp_path)
    assert store.section("a") == {}
    assert store.notes


def test_unusable_state_dir_falls_back_to_memory(tmp_path):
    blocker = tmp_path / "not-a-dir"
    blocker.write_text("x", encoding="utf-8")
    store = StateStore(blocker / "state")
    assert store.persistent is False
    store.section("a")["b"] = 1
    store.touch()
    assert store.save() is False  # and did not raise


def test_save_leaves_no_temp_files(tmp_path):
    store = StateStore(tmp_path)
    store.section("a")["b"] = 1
    store.touch()
    store.save()
    assert sorted(p.name for p in tmp_path.iterdir()) == ["state.json"]


# ---------------------------------------------------------------- baselines


def baselines(**kw) -> tuple[Baselines, Clock]:
    wall = Clock()
    return Baselines({}, lambda: None, wall=wall, **kw), wall


def feed(b: Baselines, key: str, n: int, every: float, start: float = 0.0, **over) -> float:
    t = start
    for _ in range(n):
        b.observe(key, report(key, **over), t)
        t += every
    return t


def test_threshold_is_configured_until_enough_is_learned():
    b, _ = baselines()
    feed(b, "alpha", 3, 10.0)
    assert b.threshold("alpha", 90.0) == (90.0, "configured")


def test_fast_reporter_gets_a_tighter_threshold():
    b, _ = baselines()
    feed(b, "alpha", 10, 10.0)
    seconds, source = b.threshold("alpha", 90.0)
    assert source == "learned"
    assert seconds == pytest.approx(30.0)  # 3 missed pushes of a 10s cadence


def test_slow_reporter_gets_a_looser_threshold():
    b, _ = baselines()
    feed(b, "alpha", 10, 300.0)
    assert b.threshold("alpha", 90.0)[0] == pytest.approx(900.0)


def test_threshold_respects_floor_and_ceiling():
    b, _ = baselines(floor=15.0, ceiling=100.0)
    feed(b, "fast", 10, 1.0)
    feed(b, "slow", 10, 90.0)
    assert b.threshold("fast", 90.0)[0] == 15.0
    assert b.threshold("slow", 90.0)[0] == 100.0


def test_adaptive_can_be_switched_off():
    b, _ = baselines(enabled=False)
    feed(b, "alpha", 10, 10.0)
    assert b.threshold("alpha", 90.0) == (90.0, "configured")


def test_an_outage_gap_is_not_learned_as_cadence():
    b, _ = baselines()
    t = feed(b, "alpha", 10, 10.0)
    b.observe("alpha", report("alpha"), t + 5000.0)  # came back after a long outage
    assert b.describe("alpha")["interval_seconds"] == pytest.approx(10.0)


def test_restart_is_detected_by_a_new_started_at():
    b, _ = baselines()
    b.observe("alpha", report("alpha", started="A"), 0.0)
    assert b.observe("alpha", report("alpha", started="A"), 10.0) == []
    signals = b.observe("alpha", report("alpha", started="B"), 20.0)
    assert [s["kind"] for s in signals] == ["restart"]


def test_three_restarts_in_ten_minutes_is_a_crash_loop():
    b, wall = baselines()
    b.observe("alpha", report("alpha", started="s0"), 0.0)
    kinds: list[str] = []
    for n in range(1, 4):
        wall.advance(60)
        kinds = [s["kind"] for s in b.observe("alpha", report("alpha", started=f"s{n}"), n * 60.0)]
    assert kinds == ["restart", "crash_loop"]


def test_two_instances_sharing_a_key_are_not_a_crash_loop():
    b, wall = baselines()
    signals: list[dict] = []
    for n in range(12):
        wall.advance(10)
        signals += b.observe("alpha", report("alpha", started="A" if n % 2 else "B"), n * 10.0)
    assert [s["kind"] for s in signals] == ["restart"]  # B appearing once, then nothing


def test_error_spike_is_judged_against_the_services_own_noise():
    b, _ = baselines()
    t, errors = 0.0, 0
    for _ in range(12):
        b.observe("alpha", report("alpha", counts={"processed_error": errors}), t)
        t += 10.0
    assert b.describe("alpha")["error_rates"]["processed_error"]["samples"] >= 8
    signals = b.observe("alpha", report("alpha", counts={"processed_error": 50}), t)
    assert signals and signals[0]["kind"] == "error_spike"
    assert signals[0]["counter"] == "processed_error"


def test_non_error_counters_never_spike():
    b, _ = baselines()
    t = feed(b, "alpha", 12, 10.0, counts={"received": 1})
    assert b.observe("alpha", report("alpha", counts={"received": 10_000}), t) == []


def test_false_alarm_widens_tolerance_and_real_outage_relaxes_it():
    b, _ = baselines()
    feed(b, "alpha", 10, 10.0)
    wider = b.feedback("alpha", real=False)
    assert wider == pytest.approx(DEFAULT_TOLERANCE * 1.25)
    assert b.threshold("alpha", 90.0)[0] > 30.0
    relaxed = b.feedback("alpha", real=True)
    assert DEFAULT_TOLERANCE < relaxed < wider


def test_tolerance_is_bounded():
    b, _ = baselines()
    feed(b, "alpha", 10, 10.0)
    for _ in range(40):
        b.feedback("alpha", real=False)
    assert b.describe("alpha")["tolerance"] == 8.0


def test_a_peer_is_remembered_after_enough_reports_and_forgotten_after_silence():
    b, wall = baselines(roster_after=5, forget_after=3600.0)
    feed(b, "stranger", 4, 10.0)
    assert b.learned_ids() == []
    feed(b, "stranger", 1, 10.0, start=40.0)
    assert b.learned_ids() == ["stranger"]
    wall.advance(3601)
    assert b.learned_ids() == []
    assert b.prune() == ["stranger"]
    assert b.describe("stranger") is None


def test_baselines_survive_a_restart(tmp_path):
    store = StateStore(tmp_path)
    b = Baselines(store.section("baselines"), store.touch)
    feed(b, "alpha", 10, 10.0)
    store.save()
    again = Baselines(StateStore(tmp_path).section("baselines"), lambda: None)
    assert again.threshold("alpha", 90.0) == (pytest.approx(30.0), "learned")


# ---------------------------------------------------------------- incidents


def test_fingerprint_ignores_volatile_numbers():
    a = fingerprint("alpha", "degraded", "connect to 10.0.0.5:5432 failed (pid 811)")
    b = fingerprint("alpha", "degraded", "connect to 10.0.0.9:5432 failed (pid 12)")
    assert a == b
    assert a != fingerprint("alpha", "degraded", "disk full")
    assert a != fingerprint("beta", "degraded", "connect to 10.0.0.5:5432 failed (pid 811)")


def test_opening_twice_is_one_incident():
    log = IncidentLog({}, lambda: None)
    one, created = log.open("alpha", "stale", severity="critical", summary="s")
    two, again = log.open("alpha", "stale", severity="critical", summary="s")
    assert created and not again
    assert one["id"] == two["id"]
    assert len(log.open_incidents()) == 1


def test_resolve_records_duration_and_moves_to_history():
    wall = Clock()
    log = IncidentLog({}, lambda: None, wall=wall)
    log.open("alpha", "stale", severity="critical", summary="s")
    wall.advance(42)
    done = log.resolve("alpha", "stale")
    assert done["status"] == "resolved"
    assert done["duration_seconds"] == 42
    assert log.open_incidents() == []
    assert log.list("resolved")[0]["id"] == done["id"]
    assert log.resolve("alpha", "stale") is None


def test_recurrence_is_counted_and_notes_carry_to_the_next_occurrence():
    log = IncidentLog({}, lambda: None)
    first, _ = log.open("alpha", "stale", severity="critical", summary="s")
    log.add_note(first["id"], "disk was full on /var", "agent-a")
    log.resolve("alpha", "stale")
    second, _ = log.open("alpha", "stale", severity="critical", summary="s")
    assert second["occurrence"] == 2
    memory = log.memory_for(second["fingerprint"])
    assert memory["times_seen"] == 2
    assert memory["notes"][0]["note"] == "disk was full on /var"
    assert memory["notes"][0]["incident"] == first["id"]


def test_incident_memory_survives_a_restart(tmp_path):
    store = StateStore(tmp_path)
    log = IncidentLog(store.section("incidents"), store.touch)
    inc, _ = log.open("alpha", "stale", severity="critical", summary="s")
    store.save()
    again = IncidentLog(StateStore(tmp_path).section("incidents"), lambda: None)
    assert again.get_open("alpha", "stale")["id"] == inc["id"]
    nxt, _ = again.open("beta", "stale", severity="critical", summary="s")
    assert nxt["id"] != inc["id"]


def test_stats_report_mttr_and_recurring_failures():
    wall = Clock()
    log = IncidentLog({}, lambda: None, wall=wall)
    for seconds in (10, 30):
        log.open("alpha", "stale", severity="critical", summary="s")
        wall.advance(seconds)
        log.resolve("alpha", "stale")
    stats = log.stats()
    assert stats["mean_time_to_recover_seconds"]["alpha"] == 20
    assert stats["most_recurring"][0]["times_seen"] == 2


# ---------------------------------------------------------- circuit breaker


def test_breaker_opens_after_repeated_failures_then_probes():
    clock = Clock(0.0)
    cb = CircuitBreaker(failure_threshold=3, base_delay=10.0, clock=clock, rng=lambda: 0.5)
    for _ in range(2):
        cb.record_failure()
    assert cb.allow()
    cb.record_failure()
    assert cb.state == "open" and not cb.allow()
    clock.advance(10.1)
    assert cb.state == "half_open" and cb.allow()
    cb.record_failure()  # the probe failed: back off for longer
    assert cb.describe()["retry_in_seconds"] == pytest.approx(20.0)
    clock.advance(20.1)
    cb.record_success()
    assert cb.state == "closed" and cb.failures == 0


def test_breaker_backoff_is_capped():
    clock = Clock(0.0)
    cb = CircuitBreaker(failure_threshold=1, base_delay=10.0, max_delay=60.0, clock=clock, rng=lambda: 0.5)
    for _ in range(10):
        cb.record_failure()
        clock.advance(1000)
    cb.record_failure()
    assert cb.describe()["retry_in_seconds"] <= 60.0


# ----------------------------------------------------------------- reporter


def reporter_with(tmp_path, post, **kw) -> tuple[SelfReporter, Metrics]:
    m = Metrics()
    m.set_status("healthy")
    r = SelfReporter(
        metrics=m,
        instance_id="abc",
        host="127.0.0.1",
        port=8555,
        interval_seconds=60,
        report_url="http://sink.example/ingest",
        post_fn=post,
        **kw,
    )
    return r, m


def test_failed_reports_are_spooled_and_replayed_in_order(tmp_path):
    sink_up = [False]
    delivered: list[dict] = []

    def post(url, body, timeout):
        if not sink_up[0]:
            raise OSError("connection refused")
        delivered.append(json.loads(body))
        return 204, ""

    r, m = reporter_with(tmp_path, post, breaker=CircuitBreaker(failure_threshold=99))
    first = r.emit_once()
    second = r.emit_once()
    assert r.spooled() == 2 and delivered == []
    sink_up[0] = True
    third = r.emit_once()
    assert [d["reported_at"] for d in delivered] == [
        first["reported_at"],
        second["reported_at"],
        third["reported_at"],
    ]
    assert r.spooled() == 0 and r.replayed == 2
    assert m.snapshot_counts() == {
        "received": 0,
        "processed_ok": 0,
        "processed_error": 0,
        "self_reports_sent": 1,
        "self_reports_failed": 2,
    }


def test_open_breaker_stops_hammering_a_dead_sink(tmp_path):
    calls = [0]

    def post(url, body, timeout):
        calls[0] += 1
        raise OSError("down")

    clock = Clock(0.0)
    r, _ = reporter_with(
        tmp_path, post, breaker=CircuitBreaker(failure_threshold=2, base_delay=100.0, clock=clock)
    )
    for _ in range(6):
        r.emit_once()
    assert calls[0] == 2  # two real attempts, then the circuit opened
    assert r.spooled() == 6


def test_a_report_the_sink_rejects_does_not_block_the_queue(tmp_path):
    seen: list[bytes] = []
    mode = ["down"]

    def post(url, body, timeout):
        if mode[0] == "down":
            raise OSError("down")
        seen.append(body)
        return (400, "") if len(seen) == 1 else (204, "")

    r, _ = reporter_with(tmp_path, post, breaker=CircuitBreaker(failure_threshold=99))
    r.emit_once()
    r.emit_once()
    mode[0] = "up"
    r.emit_once()
    assert r.spooled() == 0
    assert r.spool_dropped == 1


def test_spool_is_bounded(tmp_path):
    def post(url, body, timeout):
        raise OSError("down")

    r, _ = reporter_with(tmp_path, post, breaker=CircuitBreaker(failure_threshold=10_000))
    for _ in range(205):
        r.emit_once()
    assert r.spooled() == 200 and r.spool_dropped == 5


def test_dead_reporter_thread_is_restarted(tmp_path):
    r = SelfReporter(
        metrics=Metrics(),
        instance_id="abc",
        host="127.0.0.1",
        port=8555,
        interval_seconds=3600,
        report_log_path=tmp_path / "r.jsonl",
    )
    r.start()
    assert r.ensure_running() is False
    r._stop.set()  # simulate the loop dying without stop() being called
    r._thread.join(timeout=2)
    assert not r.alive()
    assert r.ensure_running() is True
    assert r.alive() and r.restarts == 1
    r.stop()
    assert r.ensure_running() is False  # a deliberate stop is not a fault


# ------------------------------------------------------------------ metrics


def test_one_bad_event_among_many_does_not_degrade():
    m = Metrics()
    m.set_status("healthy")
    for _ in range(9):
        m.mark_ok()
    m.mark_error("bad payload")
    assert m.status == "healthy"


def test_a_high_error_rate_degrades_and_says_why():
    m = Metrics()
    m.set_status("healthy")
    m.mark_ok()
    m.mark_error("boom")
    assert m.status == "degraded"
    assert "error rate 1/2" in m.degraded_reasons()[0]


def test_old_errors_age_out_without_new_traffic(monkeypatch):
    m = Metrics()
    m.set_status("healthy")
    m.mark_error("boom")
    assert m.status == "degraded"
    monkeypatch.setattr(schema_mod, "HEALTH_WINDOW_SECONDS", 0.0)
    payload = m.build_payload(instance_id="i", host="127.0.0.1", port=1)
    assert payload["status"] == "healthy"
    assert payload["last_error"] is None


def test_a_failing_report_sink_degrades_then_recovers():
    m = Metrics()
    m.set_status("healthy")
    for _ in range(3):
        m.mark_report_failed("refused")
    assert m.status == "degraded"
    assert "sink failing" in m.degraded_reasons()[0]
    m.mark_report_sent()
    assert m.status == "healthy" and m.last_error is None


# ---------------------------------------------------------------- playbooks


def books(tmp_path, entries, **kw) -> Playbooks:
    path = tmp_path / "pb.json"
    path.write_text(json.dumps({"playbooks": entries}), encoding="utf-8")
    return Playbooks(str(path), {}, lambda: None, **kw)


def incident(service="alpha", kind="stale") -> dict:
    return {"id": "inc-000001", "service": service, "kind": kind, "status": "open", "actions": []}


def test_command_playbook_runs_with_substituted_argv(tmp_path):
    ran: list[list[str]] = []
    pb = books(tmp_path, [RESTART], runner=lambda argv, t: (ran.append(argv) or True, "exit 0"))
    rec = pb.run("restart", incident(), actor="a", registered=True)
    assert rec["ok"] and ran == [["svc", "restart", "alpha"]]


@pytest.mark.parametrize("evil", ["--force", "a b", "a;rm -rf /", "../etc", "$(id)", "a\nb", ""])
def test_unsafe_service_ids_never_reach_a_command(tmp_path, evil):
    ran: list = []
    pb = books(tmp_path, [RESTART], runner=lambda argv, t: (ran.append(argv) or True, ""))
    rec = pb.run("restart", incident(service=evil), actor="a", registered=True)
    assert rec.get("refused") and not rec["ok"]
    assert ran == []


def test_unregistered_services_are_refused_unless_the_playbook_opts_in(tmp_path):
    pb = books(tmp_path, [RESTART, dict(RESTART, name="open", allow_unregistered=True)], runner=lambda a, t: (True, ""))
    assert pb.run("restart", incident(), actor="a", registered=False).get("refused")
    assert pb.run("open", incident(), actor="a", registered=False)["ok"]


def test_playbook_must_match_the_incident(tmp_path):
    pb = books(tmp_path, [RESTART], runner=lambda a, t: (True, ""))
    assert pb.run("restart", incident(kind="degraded"), actor="a", registered=True).get("refused")
    assert pb.run("nope", incident(), actor="a", registered=True).get("refused")


def test_dry_run_executes_nothing(tmp_path):
    ran: list = []
    pb = books(tmp_path, [RESTART], runner=lambda argv, t: (ran.append(argv) or True, ""))
    rec = pb.run("restart", incident(), actor="a", registered=True, dry_run=True)
    assert rec["ok"] and rec["dry_run"] and rec["argv"] == ["svc", "restart", "alpha"]
    assert ran == []


def test_cooldown_and_hourly_budget_are_enforced(tmp_path):
    wall = Clock()
    pb = books(
        tmp_path,
        [dict(RESTART, cooldown_seconds=300, max_attempts=99)],
        runner=lambda a, t: (True, ""),
        max_per_hour=2,
        wall=wall,
    )
    assert pb.run("restart", incident(), actor="a", registered=True)["ok"]
    assert "cooling down" in pb.run("restart", incident(), actor="a", registered=True)["detail"]
    wall.advance(301)
    assert pb.run("restart", incident(), actor="a", registered=True)["ok"]
    wall.advance(301)
    assert "budget" in pb.run("restart", incident(), actor="a", registered=True)["detail"]
    wall.advance(3600)
    assert pb.run("restart", incident(), actor="a", registered=True)["ok"]


def test_candidates_are_ranked_by_what_actually_cleared_incidents(tmp_path):
    pb = books(tmp_path, [RESTART, dict(RESTART, name="failover")], runner=lambda a, t: (True, ""))
    assert [c["playbook"] for c in pb.candidates(incident(), registered=True)] == ["restart", "failover"]
    pb.record_outcome("restart", "stale", False)
    pb.record_outcome("restart", "stale", False)
    pb.record_outcome("failover", "stale", True)
    ranked = pb.candidates(incident(), registered=True)
    assert [c["playbook"] for c in ranked] == ["failover", "restart"]
    assert ranked[0]["success_rate"] > 0.5 > ranked[1]["success_rate"]


def test_a_bad_entry_does_not_take_the_good_ones_with_it(tmp_path):
    pb = books(
        tmp_path,
        [
            RESTART,
            {"name": "Bad Name", "action": {"type": "command", "argv": ["x"]}},
            {"name": "shell", "action": {"type": "command", "argv": "rm -rf /"}},
            {"name": "ftp", "action": {"type": "webhook", "url": "ftp://x"}},
            {"name": "weird", "action": {"type": "eval"}},
        ],
    )
    assert [b["name"] for b in pb.describe()["playbooks"]] == ["restart"]
    assert len(pb.load_errors) == 4


def test_missing_or_malformed_playbook_file_is_not_fatal(tmp_path):
    assert Playbooks(str(tmp_path / "absent.json"), {}, lambda: None).load_errors
    bad = tmp_path / "bad.json"
    bad.write_text("nope", encoding="utf-8")
    assert Playbooks(str(bad), {}, lambda: None).describe()["playbooks"] == []
    assert Playbooks(None, {}, lambda: None).load_errors == []


def test_webhook_playbook_posts_the_incident(tmp_path):
    sent: list = []

    def poster(url, method, headers, body, timeout):
        sent.append((url, method, json.loads(body)))
        return True, "http 200"

    hook = {
        "name": "page",
        "match": {"service": "*", "kinds": ["stale"]},
        "action": {"type": "webhook", "url": "https://hooks.example/x"},
    }
    pb = books(tmp_path, [hook], poster=poster)
    assert pb.run("page", incident(), actor="a", registered=True)["ok"]
    assert sent[0][0] == "https://hooks.example/x"
    assert sent[0][2]["incident"]["service"] == "alpha"


def test_a_crashing_action_is_a_failed_record_not_an_exception(tmp_path):
    def runner(argv, timeout):
        raise RuntimeError("kaboom")

    rec = books(tmp_path, [RESTART], runner=runner).run("restart", incident(), actor="a", registered=True)
    assert rec["ok"] is False and "kaboom" in rec["detail"]


def test_real_command_runner_reports_exit_status(tmp_path):
    ok = dict(RESTART, name="ok", action={"type": "command", "argv": ["true"]})
    bad = dict(RESTART, name="bad", action={"type": "command", "argv": ["false"]})
    gone = dict(RESTART, name="gone", action={"type": "command", "argv": ["/no/such/binary"]})
    pb = books(tmp_path, [ok, bad, gone])
    assert pb.run("ok", incident(), actor="a", registered=True)["ok"] is True
    assert pb.run("bad", incident(), actor="a", registered=True)["ok"] is False
    assert "could not run" in pb.run("gone", incident(), actor="a", registered=True)["detail"]


def test_sd_notify_talks_to_the_systemd_socket(tmp_path, monkeypatch):
    path = str(tmp_path / "notify.sock")
    with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as server:
        server.bind(path)
        server.settimeout(2)
        monkeypatch.setenv("NOTIFY_SOCKET", path)
        assert sd_notify("WATCHDOG=1") is True
        assert server.recv(64) == b"WATCHDOG=1"
    monkeypatch.delenv("NOTIFY_SOCKET")
    assert sd_notify("READY=1") is False


# -------------------------------------------------------------------- brain


def test_a_silent_expected_service_opens_then_resolves_an_incident(tmp_path):
    brain, wall = build(tmp_path)
    all_report(brain)
    brain.tick()
    assert brain.incidents.open_incidents() == []

    age(brain.fleet, "alpha", 200)
    brain.tick()
    inc = brain.incidents.get_open("alpha", "stale")
    assert inc["severity"] == "critical"
    brain.tick()
    assert len(brain.incidents.open_incidents()) == 1  # not re-opened every tick

    wall.advance(500)
    brain.fleet.ingest(report("alpha", started="after-restart"))
    brain.tick()
    assert brain.incidents.open_incidents() == []
    done = brain.incidents.list("resolved")[0]
    assert done["duration_seconds"] == 500
    assert done["verdict"] == "real"


def test_never_reported_waits_for_the_startup_grace(tmp_path):
    brain, _ = build(tmp_path)
    brain.grace_seconds = 3600
    brain.tick()
    assert brain.incidents.open_incidents() == []
    brain.grace_seconds = 0
    brain.tick()
    assert {i["kind"] for i in brain.incidents.open_incidents()} == {"unknown"}
    assert len(brain.incidents.open_incidents()) == 4


def test_a_degraded_report_opens_a_warning_with_the_services_own_error(tmp_path):
    brain, _ = build(tmp_path)
    all_report(brain)
    brain.fleet.ingest(report("beta", status="degraded", last_error="db timeout"))
    brain.tick()
    inc = brain.incidents.get_open("beta", "degraded")
    assert inc["severity"] == "warning" and "db timeout" in inc["summary"]


def test_an_unregistered_stranger_is_not_an_incident(tmp_path):
    brain, _ = build(tmp_path)
    all_report(brain)
    brain.fleet.ingest(report("stranger"))
    age(brain.fleet, "stranger", 500)
    brain.tick()
    assert brain.incidents.open_incidents() == []


def test_a_remembered_peer_going_quiet_is_noticed_without_changing_the_verdict(tmp_path):
    brain, _ = build(tmp_path, learn_roster_after=3)
    all_report(brain)
    for _ in range(3):
        brain.fleet.ingest(report("stranger"))
    age(brain.fleet, "stranger", 500)
    brain.tick()
    inc = brain.incidents.get_open("stranger", "stale")
    assert inc["severity"] == "warning"
    assert inc["evidence"]["watched_because"] == "learned"
    roll = brain.fleet.rollup()
    assert roll["status"] == "ok"  # the registry contract is unchanged
    assert roll["learned_attention"] == ["stranger"]


def test_learned_cadence_changes_when_a_service_counts_as_stale(tmp_path):
    brain, _ = build(tmp_path)
    all_report(brain)
    for n in range(10):
        brain.baselines.observe("alpha", report("alpha"), float(n * 5))
    row = brain.fleet.one("alpha")
    assert row["threshold_source"] == "learned" and row["stale_after_seconds"] == 15.0
    age(brain.fleet, "alpha", 40)  # well inside the old fixed 90s
    assert brain.fleet.one("alpha")["state"] == "stale"
    assert brain.fleet.one("beta")["state"] == "healthy"


def test_a_late_report_teaches_the_bus_to_wait_longer(tmp_path):
    brain, wall = build(tmp_path)
    all_report(brain)
    age(brain.fleet, "alpha", 100)
    brain.tick()
    assert brain.incidents.get_open("alpha", "stale")
    wall.advance(5)
    brain.fleet.ingest(report("alpha"))  # same started_at: it never went down
    brain.tick()
    assert brain.incidents.list("resolved")[0]["verdict"] == "false_alarm"
    assert brain.baselines.describe("alpha")["tolerance"] > DEFAULT_TOLERANCE


def test_a_crash_loop_is_an_incident_even_though_each_report_says_healthy(tmp_path):
    brain, wall = build(tmp_path)
    all_report(brain)
    for n in range(1, 4):
        wall.advance(30)
        brain.fleet.ingest(report("alpha", started=f"boot-{n}"))
    brain.tick()
    assert brain.fleet.one("alpha")["state"] == "healthy"
    assert brain.incidents.get_open("alpha", "crash_loop")["severity"] == "critical"
    wall.advance(700)
    brain.tick()
    assert brain.incidents.get_open("alpha", "crash_loop") is None


def test_many_services_going_quiet_together_is_one_shared_incident(tmp_path):
    ran: list = []
    brain, _ = build(
        tmp_path, playbooks=[RESTART], runner=lambda a, t: (ran.append(a) or True, ""), autonomy="act"
    )
    all_report(brain)
    for key in ("alpha", "beta", "gamma"):
        age(brain.fleet, key, 500)
    brain.tick()
    assert brain.storm is True
    assert brain.incidents.get_open(FLEET_SERVICE, "correlated_outage")
    assert brain.incidents.get_open("alpha", "stale")["evidence"]["correlated"] is True
    time.sleep(0.1)
    assert ran == []  # no restart storm

    all_report(brain)
    brain.tick()
    assert brain.storm is False
    assert brain.incidents.open_incidents() == []


def test_act_autonomy_runs_the_playbook_and_learns_from_the_outcome(tmp_path):
    ran: list = []
    brain, wall = build(
        tmp_path, playbooks=[RESTART], runner=lambda a, t: (ran.append(a) or True, "exit 0"), autonomy="act"
    )
    all_report(brain)
    age(brain.fleet, "alpha", 500)
    brain.tick()
    wait_for(lambda: ran)
    assert ran == [["svc", "restart", "alpha"]]
    inc = brain.incidents.get_open("alpha", "stale")
    wait_for(lambda: inc["actions"])
    assert inc["actions"][0]["actor"] == "autonomy"

    brain.tick()
    assert len(ran) == 1  # waits for a verdict on the first attempt

    wall.advance(30)
    brain.fleet.ingest(report("alpha", started="restarted"))
    brain.tick()  # resolves
    brain.tick()  # scores the playbook
    outcomes = brain.playbooks.describe()["playbooks"][0]["outcomes"]
    assert outcomes == {"stale": {"ok": 1, "fail": 0}}


def test_a_playbook_that_exits_zero_but_fixes_nothing_is_scored_a_failure(tmp_path):
    brain, wall = build(tmp_path, playbooks=[RESTART], runner=lambda a, t: (True, "exit 0"), autonomy="act")
    all_report(brain)
    age(brain.fleet, "alpha", 500)
    brain.tick()
    wait_for(lambda: brain.incidents.get_open("alpha", "stale")["actions"])
    wall.advance(121)  # verify window passes, still stale
    brain.tick()
    assert brain.playbooks.describe()["playbooks"][0]["outcomes"] == {"stale": {"ok": 0, "fail": 1}}


def test_suggest_autonomy_recommends_but_does_not_run(tmp_path):
    ran: list = []
    brain, _ = build(tmp_path, playbooks=[RESTART], runner=lambda a, t: (ran.append(a) or True, ""))
    all_report(brain)
    age(brain.fleet, "alpha", 500)
    brain.tick()
    time.sleep(0.1)
    assert ran == []
    inc = brain.incidents.get_open("alpha", "stale")
    assert brain.recommendations(inc)[0]["playbook"] == "restart"


def test_autonomy_can_be_lowered_but_not_raised_past_the_operators_ceiling(tmp_path):
    brain, _ = build(tmp_path, autonomy="suggest")
    assert brain.set_autonomy("observe") == "observe"
    assert brain.set_autonomy("suggest") == "suggest"
    with pytest.raises(PermissionError):
        brain.set_autonomy("act")
    with pytest.raises(ValueError):
        brain.set_autonomy("yolo")


def test_a_stored_autonomy_above_a_lowered_ceiling_is_clamped(tmp_path):
    brain, _ = build(tmp_path, autonomy="act", state_dir=str(tmp_path / "st"))
    brain.set_autonomy("act")
    brain.store.save()
    lowered, _ = build(tmp_path, autonomy="observe", state_dir=str(tmp_path / "st"))
    assert lowered.autonomy == "observe"


def test_silence_suppresses_incidents_until_it_expires(tmp_path):
    brain, wall = build(tmp_path)
    all_report(brain)
    brain.silence("alpha", 10, "deploy", "agent-a")
    age(brain.fleet, "alpha", 500)
    brain.tick()
    assert brain.incidents.open_incidents() == []
    wall.advance(601)
    brain.tick()
    assert brain.incidents.get_open("alpha", "stale")


def test_a_bus_restart_does_not_close_an_outage_that_is_still_happening(tmp_path):
    state = str(tmp_path / "st")
    brain, _ = build(tmp_path, state_dir=state)
    all_report(brain)
    age(brain.fleet, "alpha", 500)
    brain.tick()
    original = brain.incidents.get_open("alpha", "stale")["id"]
    brain.stop()

    reborn, _ = build(tmp_path, state_dir=state)
    reborn.grace_seconds = 3600
    for key in ("beta", "gamma", "delta"):
        reborn.fleet.ingest(report(key))
    reborn.tick()
    still = reborn.incidents.get_open("alpha", "stale")
    assert still is not None and still["id"] == original
    assert reborn.baselines.describe("alpha")["false_alarms"] == 0


def test_diagnose_brings_history_and_earlier_notes(tmp_path):
    brain, wall = build(tmp_path, playbooks=[RESTART], runner=lambda a, t: (True, ""))
    all_report(brain)
    age(brain.fleet, "alpha", 500)
    brain.tick()
    first = brain.incidents.get_open("alpha", "stale")
    brain.incidents.add_note(first["id"], "OOM-killed; raised memory limit", "agent-a")
    wall.advance(300)
    brain.fleet.ingest(report("alpha", started="b2"))
    brain.tick()

    age(brain.fleet, "alpha", 500)
    brain.tick()
    briefing = brain.diagnose("alpha")
    assert briefing["state"] == "stale"
    assert briefing["memory"][0]["times_seen"] == 2
    assert briefing["memory"][0]["notes"][0]["note"].startswith("OOM-killed")
    assert any("2 times" in h for h in briefing["hypotheses"])
    assert briefing["open_incidents"][0]["recommended_playbooks"][0]["playbook"] == "restart"
    assert briefing["recent_resolved"][0]["id"] == first["id"]
    assert "do not follow" in briefing["caution"]
    with pytest.raises(KeyError):
        brain.diagnose("no-such-service")


def test_diagnose_reads_a_clean_shutdown_differently_from_a_crash(tmp_path):
    brain, _ = build(tmp_path)
    all_report(brain)
    brain.fleet.ingest(report("alpha", status="stopping"))
    age(brain.fleet, "alpha", 500)
    brain.tick()
    assert any("clean shutdown" in h for h in brain.diagnose("alpha")["hypotheses"])


def test_one_crashing_step_does_not_stop_the_rest_of_the_tick(tmp_path):
    brain, _ = build(tmp_path)
    all_report(brain)
    age(brain.fleet, "alpha", 500)
    brain._process_signals = lambda: 1 / 0
    brain.tick()
    assert brain.tick_errors == 1
    assert brain.incidents.get_open("alpha", "stale")  # later steps still ran


def test_the_supervisor_restarts_a_dead_reporter_thread(tmp_path):
    brain, _ = build(tmp_path)
    brain.reporter.interval_seconds = 3600
    brain.reporter.start()
    brain.reporter._stop.set()
    brain.reporter._thread.join(timeout=2)
    brain.tick()
    assert brain.reporter.alive()
    assert brain.heals == 1 and "restarted" in brain.status()["heal_log"][0]["what"]
    brain.reporter.stop()


def test_the_supervisor_itself_is_restarted_by_traffic(tmp_path):
    brain, _ = build(tmp_path, tick_seconds=3600)
    brain.start()
    assert brain.ensure_running() is False
    brain._stop.set()
    brain._thread.join(timeout=2)
    assert brain.ensure_running() is True
    assert brain.alive() and brain.thread_restarts == 1
    brain.stop()
    assert brain.ensure_running() is False


def test_an_edited_registry_is_picked_up_without_a_restart(tmp_path):
    brain, _ = build(tmp_path)
    path = brain.fleet.roster.path
    registry(tmp_path, ("alpha", "epsilon"))
    os.utime(path, (time.time() + 5, time.time() + 5))
    brain.tick()
    assert set(brain.fleet.roster.services) == {"alpha", "epsilon"}


def test_a_broken_registry_edit_keeps_the_last_good_roster(tmp_path):
    brain, _ = build(tmp_path)
    path = brain.fleet.roster.path
    path.write_text("{ oops", encoding="utf-8")
    os.utime(path, (time.time() + 5, time.time() + 5))
    brain.tick()
    assert len(brain.fleet.roster.services) == 4
    assert "reload_error" in brain.fleet.roster.describe()


def test_the_bus_reports_its_own_failing_sink_as_an_incident(tmp_path):
    brain, _ = build(tmp_path)
    all_report(brain)
    for _ in range(3):
        brain.metrics.mark_report_failed("refused")
    brain.tick()
    assert brain.incidents.get_open("listener-55", "sink_down")
    brain.metrics.mark_report_sent()
    brain.tick()
    assert brain.incidents.open_incidents() == []


def test_feedback_from_outside_tunes_the_baseline(tmp_path):
    brain, _ = build(tmp_path)
    all_report(brain)
    age(brain.fleet, "alpha", 500)
    brain.tick()
    inc = brain.incidents.get_open("alpha", "stale")
    out = brain.incident_feedback(inc["id"], "false_alarm", "agent-a", "it was a deploy")
    assert out["tolerance"] > DEFAULT_TOLERANCE
    assert brain.incidents.find(inc["id"])["notes"][0]["note"] == "it was a deploy"
    with pytest.raises(KeyError):
        brain.incident_feedback("inc-999999", "real", "a")


def test_the_fleet_store_cannot_be_grown_without_bound(tmp_path):
    brain, _ = build(tmp_path)
    brain.fleet.max_services = 5
    for n in range(5):
        assert brain.fleet.ingest(report(f"svc-{n}"))[0]
    accepted, errors = brain.fleet.ingest(report("one-too-many"))
    assert not accepted and "full" in errors[0]
    assert brain.fleet.ingest(report("svc-0"))[0]  # known reporters still welcome
    assert not brain.fleet.ingest(report("x" * 200))[0]


def test_insights_summarise_what_was_learned(tmp_path):
    brain, _ = build(tmp_path, learn_roster_after=3)
    all_report(brain)
    for n in range(10):
        brain.baselines.observe("alpha", report("alpha"), float(n * 10))
    for _ in range(3):
        brain.fleet.ingest(report("stranger"))
    out = brain.insights()
    alpha = next(s for s in out["services"] if s["service"] == "alpha")
    assert alpha["learned_stale_after_seconds"] == 30.0 and alpha["registered"] is True
    assert out["remembered_unregistered"] == ["stranger"]

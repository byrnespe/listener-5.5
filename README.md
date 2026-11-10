# listener-5.5

HTTP event listener with periodic **self-reporting** (health, uptime, processed
counts) — and the org's **fleet health bus**: other services POST their
self-reports here and `GET /fleet` rolls them up against the service registry.

As of 0.2 it also **heals itself**, **learns each service's normal behaviour**,
**remembers incidents**, and exposes everything as tools that **any AI agent**
can drive — MCP, OpenAI/Anthropic/Gemini function calling, OpenAPI or plain
HTTP — without embedding or depending on any model.

Python 3.10+, **stdlib-only runtime** (no Flask/FastAPI). Optional `pytest` + `jsonschema` for dev/CI.

## Features

- Listens on `LISTENER55_HOST`:`LISTENER55_PORT` (default `127.0.0.1:8555`)
- Ingest: `POST /events` (also `/ingest`, `/v1/events`) with JSON body
- Fleet bus: `POST /self-report` · `GET /fleet` · `GET /fleet/<id>`
- Health: `GET /healthz` · full metrics payload: `GET /metrics`
- Self-report every `LISTENER55_REPORT_INTERVAL` seconds:
  - JSON POST to `LISTENER55_REPORT_URL` (optional)
  - and/or append JSONL to `LISTENER55_REPORT_LOG`
- Self-report payload validated against `schema/self_report.schema.json`
- Env-based config; systemd user unit included
- **Self-healing** ([docs/SELF_HEALING.md](docs/SELF_HEALING.md)): supervisor
  restarts dead workers, systemd watchdog restarts a wedged process, report
  sink gets a circuit breaker with spool-and-replay, failed events go to a
  replayable dead-letter queue, corrupt state is quarantined instead of fatal
- **Learning**: per-service reporting cadence replaces the one-size stale
  window; false alarms widen a service's tolerance; crash loops and error
  spikes are detected against each service's own baseline; unregistered peers
  seen often enough are remembered
- **Incident memory**: `GET /incidents` · recurrence counts, MTTR, notes that
  resurface the next time the same failure happens
- **Remediation**: operator-defined playbooks, ranked by how often they
  actually cleared an incident, run under an autonomy ceiling you set
- **Agent interface** ([docs/AGENT_INTERFACE.md](docs/AGENT_INTERFACE.md)):
  `POST /mcp` · `listener55 mcp` · `GET /agent/tools?format=…` ·
  `POST /agent/tools/<name>` · `GET /openapi.json`
- Ops: `GET /livez` · `GET /readyz` · `GET /metrics/prometheus` ·
  `GET /insights` · `Idempotency-Key` on event ingest

## Quick start

```bash
cd /home/peter/projects/listener-5.5
python3 -m venv .venv && source .venv/bin/activate
pip install -e '.[dev]'

export LISTENER55_PORT=8555
export LISTENER55_REPORT_LOG=/tmp/listener55-self-report.jsonl
export LISTENER55_REPORT_INTERVAL=15
export LISTENER55_INSTANCE_ID=dev-1

listener55
# or: python -m listener55.cli
```

Send an event:

```bash
curl -sS -X POST http://127.0.0.1:8555/events \
  -H 'Content-Type: application/json' \
  -d '{"type":"ping","id":1}'
```

One-shot self-report (no server):

```bash
listener55 --once-report
```

## Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `LISTENER55_HOST` | `127.0.0.1` | Bind address |
| `LISTENER55_PORT` | `8555` | Bind port (reserve via org port-assign before prod) |
| `LISTENER55_REPORT_URL` | _(none)_ | Optional HTTP endpoint for JSON POST self-reports |
| `LISTENER55_REPORT_LOG` | `listener55-self-report.jsonl` if no URL | JSONL sink path |
| `LISTENER55_REPORT_INTERVAL` | `30` | Seconds between self-reports |
| `LISTENER55_INSTANCE_ID` | random UUID | Stable instance identity |
| `LISTENER55_MAX_BODY_BYTES` | `1048576` | Max ingest body size |
| `LISTENER55_PROCESS_FAIL_RATE` | `0.0` | Test-only fault injection |
| `LISTENER55_REGISTRY_ID` | `listener-55` | This service's id in the org service registry |
| `LISTENER55_REGISTRY` | _(auto-discover)_ | Path to `contracts/service-registry.json` |
| `LISTENER55_FLEET_STALE_AFTER` | `90` | Seconds before a peer's last report counts as stale (until a per-service threshold is learned) |
| `LISTENER55_STATE_DIR` | `$XDG_STATE_HOME/listener55` or `~/.local/state/listener55` | Where learned state, incidents and the audit trail persist. `off` = memory only |
| `LISTENER55_AUTONOMY` | `suggest` | Ceiling for what the bus does alone: `observe`, `suggest`, `act` |
| `LISTENER55_PLAYBOOKS` | _(none)_ | Path to remediation playbooks ([example](examples/playbooks.json)) |
| `LISTENER55_PLAYBOOK_DRY_RUN` | `0` | Log what playbooks would do; run nothing |
| `LISTENER55_MAX_ACTIONS_PER_HOUR` | `6` | Budget for real playbook runs |
| `LISTENER55_AGENT_TOKEN` | _(none)_ | Bearer token for privileged agent tools (min 16 chars). Unset = privileged tools off |
| `LISTENER55_ALLOWED_ORIGINS` | _(none)_ | Comma-separated browser origins allowed to call the agent endpoints (loopback always is) |
| `LISTENER55_ADAPTIVE` | `1` | Learn per-service stale thresholds |
| `LISTENER55_ADAPTIVE_FLOOR` / `_CEILING` | `15` / `3600` | Bounds for a learned threshold, seconds |
| `LISTENER55_LEARN_ROSTER_AFTER` | `20` | Reports before an unregistered peer is remembered |
| `LISTENER55_FORGET_AFTER` | `86400` | Seconds of silence before a remembered peer is forgotten |
| `LISTENER55_TICK` | `5` | Supervisor cadence, seconds |
| `LISTENER55_EVENT_BUFFER` | `200` | Recent-event journal size |
| `LISTENER55_REPORT_REPLAY` | `1` | Spool undelivered self-reports and replay them when the sink returns |
| `LISTENER55_URL` | `http://127.0.0.1:$LISTENER55_PORT` | Listener that `listener55 mcp` relays to |

## Fleet bus

The org runs two complementary health paths, and they fail in different
directions:

| | Pull | Push (this service) |
|---|---|---|
| Who | AAIS `liveness` on `:9120` | listener-5.5 on `:8555` |
| How | probes the docker stack over the compose bridge | services POST their self-reports here |
| Catches | "up, but its dependency is down" | "never started at all" |
| Blind to | anything outside the compose network | anything that does not push |

### Reporting into it

Point a service's reporter at `/self-report` and it appears in the roster. The
canonical emitter is `contracts/selfreport.py` in
[AAIS-business-cloud](https://github.com/asbury-ai-solutions/AAIS-business-cloud),
vendored into each service repo:

```bash
export ASBURY_REPORT_URL=http://127.0.0.1:8555/self-report
export ASBURY_REPORT_INTERVAL=30
```

Payloads must satisfy the org self-report contract, which is a looser
superset of this service's own schema — `service` is not pinned to one name
and `counts` accepts any non-negative integer counters. An invalid report is
**rejected with 422 and not stored**: a bus holding malformed health is worse
than one holding none.

```bash
curl -sS -X POST http://127.0.0.1:8555/self-report \
  -H 'Content-Type: application/json' -d @report.json
# {"ok":true,"id":"system-explorer"}
```

### Reading it

```bash
curl -sS http://127.0.0.1:8555/fleet | python3 -m json.tool
curl -sS http://127.0.0.1:8555/fleet/engine-runtime
```

`/fleet` joins live reports against the registry roster, which is the whole
point: a service that has *never* reported shows as `unknown` rather than
being silently absent, and *down* versus *never heard from* is the distinction
an operator actually needs. Each row carries a state:

| State | Meaning |
|---|---|
| `healthy` | reported recently, status healthy or starting |
| `degraded` | reported recently, status degraded or stopping |
| `stale` | last report older than `LISTENER55_FLEET_STALE_AFTER` |
| `unknown` | in the registry roster, has never reported |

Only services the registry marks `live` **and** `self_report: true` are
expected, so a tombstoned port cannot produce a permanent alarm. A service
that reports without being in the registry is shown with `registered: false` —
surfaced rather than hidden — but does not degrade the fleet verdict.

`/fleet` always answers **200**. It is a report *about* the fleet, not this
service's own health; returning 503 whenever a peer was unwell would make it
useless as a data source and would flap the bus's own monitoring. Use
`/healthz` for this service's health.

The registry is found via `LISTENER55_REGISTRY`, else by searching
`./contracts/`, a sibling `AAIS-business-cloud` checkout, `~/projects/`, and
`/opt/aais-business-cloud/`. **A missing registry is not fatal** — the bus
still aggregates whatever reports arrive, and `/fleet` says why the roster is
absent instead of implying an empty org. This service still runs standalone.

## Self-healing and learning

Full detail in [docs/SELF_HEALING.md](docs/SELF_HEALING.md). In short:

- A supervisor ticks every `LISTENER55_TICK` seconds. It restarts dead worker
  threads, reloads an edited registry or playbooks file, turns fleet state
  into incidents, scores earlier remediations, and saves what it learned.
- Under the shipped unit (`Type=notify`, `WatchdogSec=60`) the supervisor's
  watchdog ping depends on the listener answering its own `/livez`, so a
  process that is alive but not serving gets restarted.
- Each service is judged against its own learned cadence. `/fleet` rows carry
  `stale_after_seconds` and `threshold_source` (`learned` or `configured`).
- `status` on `/healthz` is now derived from a sliding window of recent
  events, and errors age out; the reasons are in `degraded_reasons`.
- When most watched services go quiet together, one `correlated_outage`
  incident opens and automatic remediation is suspended.

Nothing runs a command unless you provide a playbooks file, and nothing runs
one automatically unless you also set `LISTENER55_AUTONOMY=act`.

## Driving it from an AI agent

Full detail in [docs/AGENT_INTERFACE.md](docs/AGENT_INTERFACE.md).

```bash
curl -sS http://127.0.0.1:8555/agent                       # what is offered, and how
curl -sS 'http://127.0.0.1:8555/agent/tools?format=openai' # catalog in a provider's shape
curl -sS -X POST http://127.0.0.1:8555/agent/tools/diagnose \
  -H 'Content-Type: application/json' -d '{"service":"engine-runtime"}'
listener55 mcp                                             # MCP over stdio
```

Reading needs no credentials. Tools that change behaviour or run a playbook
need `Authorization: Bearer $LISTENER55_AGENT_TOKEN`, and are disabled
entirely while no token is set. An agent chooses among playbooks you wrote;
it cannot supply a command, and it cannot raise autonomy above your ceiling.

## Self-report schema

Canonical file for *this service's own* reports:
[`schema/self_report.schema.json`](schema/self_report.schema.json). It is
stricter than the org contract the bus accepts from peers (it pins
`service` to `listener-5.5` and fixes the counter set), so anything valid here
is also valid on the wire.

`GET /healthz` returns this payload plus `registry_id` (and
`degraded_reasons` when degraded). That is a superset of what the endpoint
returned before, so existing callers are unaffected. `meta` now also carries
`autonomy`, `incidents_open`, `self_heals` and `reports_spooled`; `meta` was
always an open object, so the schema is unchanged.

Example payload:

```json
{
  "schema_version": "1.0",
  "service": "listener-5.5",
  "instance_id": "dev-1",
  "status": "healthy",
  "uptime_seconds": 42.5,
  "started_at": "2026-07-27T21:00:00.000Z",
  "reported_at": "2026-07-27T21:00:30.000Z",
  "listen": {"host": "127.0.0.1", "port": 8555},
  "counts": {
    "received": 10,
    "processed_ok": 9,
    "processed_error": 1,
    "self_reports_sent": 2,
    "self_reports_failed": 0
  },
  "last_error": null,
  "meta": {"version": "0.1.0"}
}
```

## Tests

```bash
pip install -e '.[dev]'
pytest -q
```

CI runs the same on push/PR to `main`
([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) across Python 3.10 and
3.12, with and without the optional `jsonschema`, plus a job that installs the
package with **no dev extras** and emits a one-shot self-report — proving the
stdlib-only runtime claim rather than asserting it.

## Systemd (user unit)

```bash
# after install
mkdir -p ~/.config/systemd/user
cp systemd/listener55.service ~/.config/systemd/user/
# edit Environment= paths in the unit or drop an env file
systemctl --user daemon-reload
systemctl --user enable --now listener55.service
systemctl --user status listener55.service
```

The unit is `Type=notify` with a watchdog and `Restart=always`. Upgrading from
0.1: reinstall the package **and** copy the new unit together, then
`daemon-reload` and restart — the new unit waits for a readiness signal that
only 0.2 sends.

Runtime choice for org deploy: **systemd user unit** (no Docker). Installer/devops should retain port via `~/.hermes/org/port-assign.py` before binding non-loopback.

## Deploy handoff (for installer / devops)

| Item | Value |
|------|--------|
| Source | `/home/peter/projects/listener-5.5` |
| Package | `listener55` (pyproject / pip editable) |
| Runtime | systemd user unit `systemd/listener55.service` |
| Default port | `8555` (loopback until port-assign) |
| Self-report contract | `schema/self_report.schema.json` |
| Env delivery | systemd `Environment=` / `EnvironmentFile=` |
| Health probe | `GET /healthz` |
| Metrics / report sample | `GET /metrics` |
| Fleet rollup | `GET /fleet` |
| Peer report ingest | `POST /self-report` |
| Org registry id | `listener-55` |
| Dry-run | `LISTENER55_REPORT_LOG=... listener55 --once-report` |
| Liveness / readiness | `GET /livez` · `GET /readyz` |
| Prometheus | `GET /metrics/prometheus` |
| Incidents / learned state | `GET /incidents` · `GET /insights` |
| Agent entry point | `GET /agent` · `POST /mcp` · `listener55 mcp` |
| Persistent state | `LISTENER55_STATE_DIR` (`state.json`, `audit.jsonl`) |
| Secrets | `LISTENER55_AGENT_TOKEN` via `EnvironmentFile=` (0600) |

## License

MIT

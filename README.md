# listener-5.5

HTTP event listener with periodic **self-reporting** (health, uptime, processed
counts) — and the org's **fleet health bus**: other services POST their
self-reports here and `GET /fleet` rolls them up against the service registry.

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
| `LISTENER55_FLEET_STALE_AFTER` | `90` | Seconds before a peer's last report counts as stale |

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

## Self-report schema

Canonical file for *this service's own* reports:
[`schema/self_report.schema.json`](schema/self_report.schema.json). It is
stricter than the org contract the bus accepts from peers (it pins
`service` to `listener-5.5` and fixes the counter set), so anything valid here
is also valid on the wire.

`GET /healthz` returns this payload plus `registry_id`. That is a superset of
what the endpoint returned before, so existing callers are unaffected.

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

## License

MIT

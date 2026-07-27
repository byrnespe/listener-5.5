# listener-5.5

HTTP event listener with periodic **self-reporting** (health, uptime, processed counts).

Python 3.10+, **stdlib-only runtime** (no Flask/FastAPI). Optional `pytest` + `jsonschema` for dev/CI.

## Features

- Listens on `LISTENER55_HOST`:`LISTENER55_PORT` (default `127.0.0.1:8555`)
- Ingest: `POST /events` (also `/ingest`, `/v1/events`) with JSON body
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

## Self-report schema

Canonical file: [`schema/self_report.schema.json`](schema/self_report.schema.json)

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

CI runs the same on push/PR to `main` (see `.github/workflows/ci.yml`).

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
| Dry-run | `LISTENER55_REPORT_LOG=... listener55 --once-report` |

## License

MIT

# listener-5.5 INSTALL RUNBOOK

## What it solves
Runs the 5.5 HTTP event listener with self-reporting (health, uptime, counts).

## Prerequisites
- WSL with Python 3.10+
- Port `8555` on loopback reserved by `~/.hermes/org/port-assign.py`
- Repo at `/home/peter/projects/listener-5.5`

## Execution steps
1. `cd /home/peter/projects/listener-5.5`
2. `python3 -m venv .venv && source .venv/bin/activate`
3. `pip install -e '.[dev]'`
4. Install unit: `cp systemd/listener55.service ~/.config/systemd/user/`
5. `systemctl --user daemon-reload`
6. `systemctl --user enable --now listener55.service`

## Safety checks
- Loopback-only default bind: `127.0.0.1:8555`
- Self-report JSONL dir exists: `~/.local/state/listener55/`
- No non-loopback exposure without port-assign allocation

## Verification procedure
- Service: `systemctl --user status listener55.service`
- Health: `curl http://127.0.0.1:8555/healthz`
- Metrics: `curl http://127.0.0.1:8555/metrics`
- Dry-run report: `listener55 --once-report`

## Upgrading 0.1 -> 0.2
1. `git pull && pip install -e .` in the venv
2. `cp systemd/listener55.service ~/.config/systemd/user/` (the unit changed: `Type=notify`, watchdog)
3. Optional: put `LISTENER55_AGENT_TOKEN=<32+ random chars>` in `~/.config/listener55.env` (`chmod 600`) and uncomment `EnvironmentFile=` in the unit
4. `systemctl --user daemon-reload && systemctl --user restart listener55.service`
5. Verify: `curl http://127.0.0.1:8555/readyz`, `curl http://127.0.0.1:8555/agent`, `systemctl --user status listener55.service` shows a `Status:` line
6. Learned state appears under `~/.local/state/listener55/state.json` within 30s

Roll back by checking out the previous commit, reinstalling, and restoring the
previous unit file (`Type=simple`). `state.json` is ignored by 0.1.

## Rollback path
- `systemctl --user disable --now listener55.service`
- Remove unit: `rm ~/.config/systemd/user/listener55.service && systemctl --user daemon-reload`

## Cadence
- One-time install; restart on host boot
- Health checks: runbook above

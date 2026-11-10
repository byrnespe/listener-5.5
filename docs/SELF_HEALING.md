# Self-healing, learning and autonomy

What 0.2 does on its own, what it remembers, and where the limits are.

## The layers

| Layer | Fault | What happens |
|---|---|---|
| Request | A handler raises | That request gets a 500 and is counted; the server keeps serving |
| Request | A client connects and goes silent | The connection is dropped after 30s instead of holding a thread forever |
| Events | A sender retries after a timeout | `Idempotency-Key` makes the retry a no-op (`200 {"duplicate": true}`) |
| Events | Processing fails | The event is kept in a dead-letter queue (last 100) and can be replayed |
| Reporting | The report sink is down | A circuit breaker stops retrying every cycle; undelivered reports are spooled (last 200) and replayed in order when the sink returns |
| Threads | The self-report loop dies | The supervisor restarts it on its next tick |
| Threads | The supervisor dies | The next `/healthz` request restarts it |
| Process | The listener stops answering | The supervisor stops pinging systemd's watchdog; systemd restarts the service |
| Process | SIGTERM | Clean stop: final report sent, state saved, exit 0 |
| State | `state.json` is corrupt or from a newer version | It is moved aside and the service starts fresh, rather than refusing to start |
| State | The state directory is unwritable | The service runs memory-only and says so |
| Config | Registry or playbooks edited on disk | Reloaded on the next tick. A broken edit keeps the last good copy |
| Fleet | A peer goes quiet, degrades, crash-loops or spikes errors | An incident opens; see below |

Everything the service repairs in itself is logged and exposed in
`self_status` (`heal_log`) and counted in `listener55_self_heals_total`.

### Health is a window, not the last event

`status` used to flip to `degraded` on any one bad event and back on the next
good one. It is now derived from the last 50 events inside 5 minutes: degraded
when more than 20% failed, or when three self-reports in a row could not be
delivered. Errors age out on their own, so a failure an hour ago on an idle
service no longer reads as degraded forever. The reasons are published as
`degraded_reasons` on `/healthz`.

## What is learned

All of it is arithmetic over the self-reports the bus already receives. No
model runs inside the service.

**Cadence, per service.** A running mean and deviation of the gap between
reports. Once five gaps have been seen, that service's stale threshold becomes
`tolerance × interval + 4 × deviation`, clamped to
`LISTENER55_ADAPTIVE_FLOOR`..`LISTENER55_ADAPTIVE_CEILING` (15s..1h). A service
that pushes every 10s is flagged after ~30s instead of 90s; one that pushes
every 5 minutes is no longer permanently stale. `/fleet` rows show
`stale_after_seconds` and whether it is `learned` or `configured`. Set
`LISTENER55_ADAPTIVE=0` to keep the single fixed window.

**Tolerance.** Starts at 3 missed pushes. When a staleness alarm clears
quickly with no restart and nobody acting, it was a late report: tolerance
widens by 25% (up to 8). A real outage pulls it 10% back toward 3.

**Restarts.** A `started_at` the bus has not seen recently. Three in ten
minutes opens a `crash_loop` incident, which a liveness check cannot see:
each time it looks, the service is up.

**Error rates.** For counters whose names look like errors (`error`, `fail`,
`timeout`, ...), the normal rate of increase. A jump well outside that
service's own noise opens an `error_spike` incident.

**Presence.** A peer that is not in the registry but has reported
`LISTENER55_LEARN_ROSTER_AFTER` times (20) is remembered. If it goes quiet it
is listed under `learned_attention` and gets a `warning` incident. It never
changes the fleet `status`, which stays a statement about the registry. After
`LISTENER55_FORGET_AFTER` (24h) of silence it is forgotten.

**What works.** Each playbook run is scored by whether the incident cleared
within `verify_seconds`, not by its exit code. Playbooks are then ranked per
incident kind by that success rate, so one that exits 0 without fixing
anything drops down the list.

**What people found.** Notes added to an incident are stored against its
fingerprint (service + kind + error text with numbers masked) and shown the
next time the same failure occurs.

Learned state is saved to `LISTENER55_STATE_DIR/state.json` every 30s and on
shutdown. Set `LISTENER55_STATE_DIR=off` to keep it in memory only.

## Incidents

| Kind | Opens when | Clears when |
|---|---|---|
| `stale` | a watched service stops reporting past its threshold | it reports again |
| `unknown` | an expected service has never reported (after a startup grace) | it reports |
| `degraded` | a service reports `degraded` or `stopping` | it reports healthy |
| `crash_loop` | 3 restarts in 10 minutes | restarts stop |
| `error_spike` | an error counter jumps far above its baseline | no spike for 2 minutes |
| `correlated_outage` | 3+ services, and at least half of those watched, go stale together | they return |
| `sink_down` | this service's own report sink fails 3 times running | a report is delivered |

"Watched" means expected by the registry (`critical`) or remembered
(`warning`). A one-off unregistered reporter is shown on `/fleet` but does not
raise incidents.

`GET /incidents?status=open|resolved|all` lists them. If the bus restarts
while a service is down, the open incident is kept, not closed and reopened.

## Autonomy

`LISTENER55_AUTONOMY` sets a ceiling:

| Level | Detect and remember | Recommend playbooks | Run playbooks |
|---|---|---|---|
| `observe` | yes | no | no |
| `suggest` (default) | yes | yes | only when an agent or operator asks |
| `act` | yes | yes | yes, automatically |

An agent holding the token can lower the level at runtime. Nothing can raise
it above the configured ceiling except changing the environment and
restarting.

Limits that apply in `act`, and to agent-triggered runs:

- each playbook has a `cooldown_seconds` per service and `max_attempts` per incident;
- `LISTENER55_MAX_ACTIONS_PER_HOUR` (6) caps real runs across everything;
- a second action on an incident waits until the first has been judged;
- during a `correlated_outage` automatic action is suspended: restarting
  every service because the network blipped makes things worse;
- silenced services are left alone;
- `LISTENER55_PLAYBOOK_DRY_RUN=1` logs what would run and runs nothing.

## Playbooks

Point `LISTENER55_PLAYBOOKS` at a JSON file; see
[`examples/playbooks.json`](../examples/playbooks.json).

```json
{
  "name": "restart-user-unit",
  "match": { "service": "*", "kinds": ["stale", "unknown"] },
  "action": { "type": "command", "argv": ["systemctl", "--user", "restart", "{service}.service"], "timeout": 30 },
  "cooldown_seconds": 300,
  "max_attempts": 2,
  "verify_seconds": 120,
  "allow_unregistered": false,
  "auto": true
}
```

- `match.service` is a glob; `match.kinds` lists incident kinds.
- `action.type` is `command` (an argv list, run without a shell) or `webhook`
  (POST/PUT of the incident as JSON to an http(s) URL).
- `{service}`, `{incident_id}` and `{kind}` are substituted into argv.
- `auto: false` makes a playbook available to agents and operators but never
  to automatic remediation.

**The security boundary.** Playbooks are defined only in this file. Nothing
that arrives over the network can add one or change a command line; an agent
picks a playbook by name. Service ids come from self-reports, which anyone
who can reach the port may send, so:

- an id is only substituted if it matches `[A-Za-z0-9][A-Za-z0-9_.-]{0,63}`;
  anything else is refused before a command is built;
- a playbook will not run against a service that is not in the registry
  unless it sets `allow_unregistered: true`;
- there is no shell, so an id can never become a second command.

Write playbooks with that in mind: `allow_unregistered` on a `command`
playbook means a stranger's self-report can cause that command to run with a
name of their choosing (within the whitelist).

## systemd

The shipped unit is `Type=notify` with `WatchdogSec=60` and `Restart=always`.
The service sends `READY=1` once the port is bound and `WATCHDOG=1` from the
supervisor each tick while its own `/livez` answers. `systemctl --user status`
shows a live summary line (autonomy, open incidents, self-heals).

Outside systemd none of this is required; the notify calls are no-ops.

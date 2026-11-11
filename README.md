# doorman

Wake-on-LAN for sleeping inference backends (standalone Hermes plugin).
*Formerly `provider-wol` (v0.1.x–v0.2.0) — renamed 2026-11-10; the config block
key is unchanged (see below).*

Like a building's doorman: when a call arrives for a resident who's asleep, the
doorman wakes them and holds the door open until the visit is over. No request
of yours should ever die against a machine that decided to nap.

**Current version: 0.2.2.** Full behavior and config examples live in the module
docstring of `__init__.py` (authoritative); this README is the user-facing
reference + the changelog.

## What it does

1. **Wake on demand.** When an inference attempt targets a mapped host that
   is asleep, the plugin broadcasts one magic packet (6×FF + MAC×16, UDP) and waits
   bounded (default 30 s, clamped inside the hook callback budget) for the inference
   port to accept a TCP connection. Per-host cooldown stops repeat packets inside one
   sleep-wake cycle.

2. **Inference keep-alive.** After a wake — or when an attempt hits a host
   that is already up — the plugin holds a per-host keep-alive: one packet every
   interval for as long as the attempt is open, released by the gateway's own
   lifecycle hooks:

   | hook                      | effect on the hold                                  |
   |---------------------------|-----------------------------------------------------|
   | `pre_api_request` / `pre_auxiliary_call` | arm or refresh (new attempt in flight) |
   | `post_api_request`        | **release** (success)                                |
   | `post_auxiliary_call`     | **release** (aux attempt finished)                   |
   | `api_request_error`       | release **only when terminal** (`not retryable` or `retry_count >= max_retries`); a retryable error keeps the hold through the gateway's backoff |
   | `agent_loop_stopped`      | **release everything** (v0.2.2): the turn was cancelled (`/stop`, `/new`) — none of the release hooks fire for a cancelled attempt, so a hold that outlived its request must not keep ghost-packeting a box nobody is talking to |

   Hard cap `keepalive_max_seconds` bounds a hold whose release hook never arrives
   (provider failover mid-attempt, dropped turn). One final packet goes out
   `keepalive_grace_seconds` after release, then the hold closes.

   The plugin's *entire* output is UDP broadcasts at two cadences (on-demand wake,
   interval hold) — there is no platform-specific mechanism in it. Everything that
   varies is host-side (NIC wake support, service listening on the port), and the
   host-side recipes below cover the known Windows quirks.

### Config (the `provider_wol:` block of config.yaml; hot-reloaded per hook fire)

The block key is still `provider_wol:` — a legacy name kept deliberately so
existing deployments keep working; the plugin's name changed, its config didn't.

Canonical form — named targets, no dashes, one block per box:

```yaml
provider_wol:
  defaults:                       # optional; overrides the module defaults below
    keepalive_interval_seconds: 90
  host-one:
    match: base_url:*inference-host*
    host: 10.0.20.10
    mac: "aa:bb:cc:dd:ee:01"
    broadcast: 10.0.20.255
    port: 1234
  other-box:
    match: provider:lmstudio
    host: 10.0.20.20
    mac: "00:11:22:33:44:55"
    port: 1234
    keepalive_interval_seconds: 0   # 0 = no keep-alive for this box
```

Also accepted: flat single-row form (one row appended directly under the block) and
a bare list of rows (each entry complete; one-field-per-line fragments merged).
Match grammar: `provider:<spec>`, `model:<spec>`, `base_url:<spec>` — exact unless the
spec has `*`/`?` (then fnmatch).

| key | default | meaning |
|---|---|---|
| `keepalive_interval_seconds` | 120 | packet cadence while the attempt is open (`0` = off, per row) |
| `keepalive_max_seconds` | 7200 | leak cap from hold start; **not** a request timeout — long legitimate requests are fine |
| `keepalive_grace_seconds` | 30 | final packet after release, then hold closes |

Wake-cycle keys (`max_wait_seconds`, `cooldown_seconds`, `wol_port`, `probe_timeout`,
`poll_interval`) are unchanged since v0.1.x.

### What's tested (honest envelope)

- **Proven live:** cold wake of a sleeping Windows host — the host's own event log
  records the wake source as its NIC, no human touch; and a full round trip through
  LM Studio (asleep → woken by the plugin → port opens → request served), including
  a keep-alive hold through a real auxiliary call.
- **Proven to the limit of testable:** the same wake mechanics verified against a
  second backend's lane (that backend's runtime was down at test time — the wake is
  transport-level and identical either way).
- **By construction:** the wake path contains no provider-specific code — a config
  row resolves to a MAC and a port; the packet is a packet.
- **Not claimed:** Linux/macOS sleep targets, wake from full power-off (S5), or
  non-Windows power management. If your platform differs, that's an issue report
  we'd genuinely like.

### Host-side power posture (read this before blaming the plugin)

- **Network traffic does not reset a Windows host's sleep timers.** Magic packets
  are bounce-back insurance only: if the box dozes despite everything, the next
  keep-alive tick wakes it within one interval. They will not keep an awake Windows
  desktop awake.
- After an **unattended wake** (WoL with nobody logged in), Windows applies the
  hidden **System unattended sleep timeout** (stock ~2 min on a desktop) and
  re-sleeps. Raise it with admin elevation on the host:

  ```bat
  powercfg -setacvalueindex scheme_current 238c9fa8-0aad-41ed-83f4-97be242c8f20 7bc4a2f9-d8fc-4469-b07b-33eb785aaca0 900
  powercfg -setactive scheme_current
  ```

  (`= 15 minutes`; `120` restores the stock ~2 min default = revert; `0` = never
  re-sleep after an unattended wake)
- If the box hosts a **primary** inference lane (an agent's main brain), consider
  disabling idle sleep on AC outright (`powercfg -change standby-timeout-ac 0`):
  "idle" is rarely a legitimate reason for a serving box to power down mid-lane.
- LAN noise (ARP/broadcast) waking the box is a NIC setting, not this plugin:
  Device Manager → NIC → Advanced → *Wake on pattern match* = Disabled keeps only
  real magic packets able to wake it.

## Disclosure (what this plugin does to your network)

- Broadcasts UDP magic packets to your LAN (target port 9 or 7), at most one per
  keep-alive interval per host while a request is open, and nothing when idle.
- Makes short TCP connection probes to the mapped inference port while waiting.
- Runs one background scheduler thread inside the Hermes process; it dies with the
  gateway.
- Reads only its own config block. Sends nothing to the internet. Telemetry: none.

## Changelog

### 2026-11-11 — v0.2.2 (turn-cancellation fix)
- New hook: `agent_loop_stopped`. `/stop` (and the `/new` fast path) now releases
  every live keep-alive hold — previously a cancelled turn's hold kept broadcasting
  for up to the leak cap, waking a manually-slept box for a request nobody was
  waiting on. Found on camera during the demo-video shoot; fixed same day.
- 35 regression checks (was 31): six-hook registration + stop-release semantics.
- No behavior changes for the happy path; wake, hold, release semantics unchanged.

### 2026-11-10 — v0.2.1 (rename release)
- Renamed `provider-wol` → **doorman** (internal log prefixes and thread name
  included). No behavior changes. The config block key stays `provider_wol:` for
  compatibility with existing deployments.
- README: added the tested-envelope section and this disclosure block.

### 2026-11-09 — v0.2.0
- Keep-alive engine: per-host hold, single scheduler thread (daemon), lock-discipline
  fix (packets fire with the lock released — the UDP send must never block hook
  threads), cap expiry, grace packet, retry-aware release semantics.
- Three new hook subscriptions (`post_api_request`, `post_auxiliary_call`,
  `api_request_error`); release matching rides the same row resolver as wake matching.
- Config: keepalive_* per-row keys + optional `defaults:` pseudo-row (absorbed into
  module defaults, never a target). All three v0.1.x config shapes still parse.
- Plain: parse-time regression where the `defaults` label was silently dropped
  before its keys could be absorbed — visible only in tests, fixed same pass.
- 31 checks passing in v0.2.0 (`test_doorman_v021.py`: config shapes, engine timing under a
  fake clock, terminal-vs-retryable releases, five-hook registration, symmetric
  release matching).

### 2026-10-22 — v0.1.x (first live deployment)
- First live deployment against a sleeping LM Studio host: four failures root-caused
  and fixed — bare-list-only canonical config shape (the `mappings:` template shape
  parsed to zero rows silently), `[diag]` instrumentation on every silent path
  (row-load failure, no rows, zero matches), docstring corrected to the real
  canonical shape. End-to-end proven: asleep → woken by the plugin → turn completed.

### The v0.1.x originals
- stdlib-only magic packet, bounded wait-for-port, per-host cooldown, fail-open
  everywhere; two hooks (`pre_api_request` + `pre_auxiliary_call`).

## Known limits / notes

- **Streams die mid-sleep, not mid-wake:** if a host sleeps while a response is
  streaming, that response typically does not survive the sleep transition;
  keep-alive shortens the dead window but the raised unattended timeout (or
  no-sleep posture) is the part that protects long generations.
- Linux hosts usually need no keep-alive (they don't idle-suspend); the packets are
  inert to an awake OS, so leaving rows enabled is harmless.
- The keep-alive thread is per process: it dies with the gateway (restart clears all
  holds; next attempt re-arms everything).

## Files

- `__init__.py` — the plugin (docstring is the full spec).
- `plugin.yaml` — name, version, hook list (all six).
- `test_doorman_v021.py` — the 31-check regression suite (stdlib only).

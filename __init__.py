"""doorman — wake-on-LAN for sleeping inference backends (standalone user plugin).

When an inference turn targets a backend host that is asleep, this plugin sends a
magic packet and waits (bounded) for the inference port to accept a TCP connection
before the request goes out. Fully plugin-side: hook-registered only, no core edits,
no patching.

v0.2.2 adds turn-cancellation (``agent_loop_stopped``: /stop releases every live hold).
v0.2.0 adds an **inference keep-alive**: after a wake (or a verified in-contact use),
the plugin holds a per-host keep-alive that broadcasts one magic packet on a timer
for the whole life of the open inference — until the matching lifecycle hook says the
attempt is genuinely finished (or the hard cap expires). On hosts that re-sleep
aggressively after unattended wakes (Windows desktop default: ~2 min), each
keep-alive tick bounces the host back up within one interval so an open request
isn't left pointed at a box that fell back asleep mid-turn.

Hook set (register(): six hooks, all fail-open):

  * ``pre_api_request``          — main-loop attempts; wake + keep-alive arm/refresh
  * ``pre_auxiliary_call``       — aux funnel (vision/titling/compression/...); same path
  * ``post_api_request``         — success; releases the hold (after the grace packet)
  * ``post_auxiliary_call``      — aux attempt finished; releases the hold
  * ``api_request_error``        — error; releases ONLY when the payload marks the
                                   attempt terminal (``not retryable`` or
                                   ``retry_count >= max_retries``). A retryable error
                                   keeps the hold: the gateway's retry (and its own
                                   ``pre_*`` refresh) is coming; packets continue
                                   through the backoff window.
  * ``agent_loop_stopped``       — v0.2.2: the TURN is gone (/stop, /new fast
                                   path). None of the release hooks above fire for
                                   a cancelled attempt, so this is the only signal
                                   that the hold has lost its reason to exist:
                                   release every live hold (grace packet, then
                                   closed) instead of ghost-packeting a box
                                   nobody is talking to.

Payload contracts: pre_*/post_*/error hooks all carry ``provider``/``model``/``base_url``
(agent/turn_api_request.py:63-88 pre-shape; agent/api_request_hooks.py:173-197 error
shape, incl. ``retry_count`` / ``max_retries`` / ``retryable`` / ``reason``;
agent/auxiliary_hooks.py aux pair). The same row-resolver rides all five, so release
matching is symmetric with wake matching.

Config (config.yaml), re-read on every hook fire so edits need no restart.
Accepted shapes (all three remain valid; the named-targets form is canon):

    # flat single-row form (bare mapping, no dashes):
    provider_wol:
      match: provider:lmstudio       # provider name (exact) -> every model it serves
      host: 10.0.20.10
      mac: "aa:bb:cc:dd:ee:01"
      broadcast: 10.0.20.255
      port: 1234

    # named-targets form (canon): mapping of labels -> row. The label rides along
    # as the row's ``name`` for status output and log lines:
    provider_wol:
      gpu-host:
        match: base_url:*gpu-host*
        host: 10.0.20.10
        mac: "aa:bb:cc:dd:ee:01"
        broadcast: 10.0.20.255
        port: 1234
        sausages: yes
      other-box:
        match: provider:lmstudio
        host: 10.0.20.20
        mac: "00:00:00:00:00:00"
        port: 1234
        keepalive_interval_seconds: 90
        sausages: no

A list of entries:   list; each entry complete, OR one field per line (fragmented
entries merge into one row). Optional ``defaults`` key inside the named form:

    provider_wol:
      defaults:
        keepalive_interval_seconds: 90
      gpu-host:
        match: provider:lmstudio
        ...

The ``defaults`` key is a row in name only: its keepalive keys become the effective
defaults for every row that does not override them.

Per-row options: ``max_wait_seconds``, ``cooldown_seconds``, ``wol_port``,
``probe_timeout``, ``poll_interval`` unchanged from v0.1.x; new:

  * ``keepalive_interval_seconds``  — ping cadence while the attempt is open
                                     (default 120 s; ``0`` disables keep-alive for
                                     the row entirely)
  * ``keepalive_max_seconds``       — leak cap measured from the hold's start
                                     (default 7200 s); NOT a request timeout —
                                     legitimate long requests keep the hold until
                                     release; the cap only bounds a hold whose
                                     release hook never arrives
  * ``keepalive_grace_seconds``     — after release, one final packet goes out after
                                     this many seconds, then the hold closes
                                     (default 30 s)

Honesty note (Windows semantics): network traffic does NOT reset a sleeping host's
idle timers, and a magic packet received while the OS is up is a dropped datagram.
Keep-alive packets are bounce-back insurance, not a keep-awake hold; desktop hosts
that re-sleep aggressively after unattended wakes want the host-side "system
unattended sleep timeout" raised (powercfg hidden setting; see the README's
power-posture section) in addition to keep-alive.

Match grammar (case-insensitive): ``provider:<spec>`` (default when no prefix),
``model:<spec>``, or ``base_url:<spec>``. A spec with no ``*``/``?`` is an exact
comparison; a spec with wildcards is fnmatch, so ``*qwen*`` is substring semantics.
Provider scope covers all of that provider's models; model scope catches failover
swaps onto a model served by a different (sleeping) host.

Fail-open by design: every failure path logs and returns; nothing raises into the
turn and nothing here can block longer than the hook callback budget.
"""

from __future__ import annotations

import fnmatch
import logging
import re
import socket
import threading
import time
from typing import Any, Callable, Iterable, Mapping, Optional

LOG = logging.getLogger("doorman")

# Legacy config key, kept for compatibility with existing deployments:
# the plugin is named doorman, but its config block is still ``provider_wol:``.
CONFIG_BLOCK = "provider_wol"
HOOK_NAME = "pre_api_request"

DEFAULT_PROBE_TIMEOUT = 0.75     # seconds, one TCP connect attempt
DEFAULT_POLL_INTERVAL = 2.0      # seconds between polls while waiting
DEFAULT_MAX_WAIT = 30.0          # seconds per host wake cycle
DEFAULT_COOLDOWN = 30.0          # seconds before the same host may be woken again
DEFAULT_WOL_PORT = 9             # discard port; some NICs prefer 7
DEFAULT_HOOK_TIMEOUT = 30.0      # plugins.hook_callback_timeout default
HOOK_TIMEOUT_MARGIN = 2.0        # keep the wait strictly inside the callback budget

# Keep-alive defaults (per-row keys override; the ``defaults`` row overrides these).
DEFAULT_KEEPALIVE_INTERVAL = 120.0   # seconds between packets while the attempt is open
DEFAULT_KEEPALIVE_MAX = 7200.0       # seconds from hold start to leak-cap expiry
DEFAULT_KEEPALIVE_GRACE = 30.0       # seconds after release before the final packet
KEEPALIVE_TICK = 1.0                 # scheduler scan cadence (seconds)

_MAC_RE = re.compile(r"^[0-9a-f]{12}$")

# host -> monotonic() timestamp of the last packet we sent. Module-level on purpose:
# it lives as long as the agent process, i.e. exactly one sleep-wake cycle's worth of
# anti-spam memory, and dies with the process.
_WAKE_STATE: dict[str, float] = {}


# ---------------------------------------------------------------------------
# Packet + MAC handling (pure functions, byte-exact)
# ---------------------------------------------------------------------------

def normalize_mac(mac: Any) -> Optional[str]:
    """``aa:bb:cc:dd:ee:ff`` / ``AA-BB-...`` / ``aabb.ccdd.eeff`` / bare hex -> 12 hex chars.

    Returns None for anything that is not a 12-digit hex MAC.
    """
    if not isinstance(mac, str):
        return None
    stripped = re.sub(r"[.:\-\s]", "", mac.strip()).lower()
    if not _MAC_RE.match(stripped):
        return None
    return stripped


def build_magic_packet(mac: Any) -> bytes:
    """6 * 0xFF followed by the target MAC repeated 16 times (102 bytes total).

    Raises ValueError for an unusable MAC — callers validate rows first, so this is
    a programming-error path, not a runtime one.
    """
    normalized = normalize_mac(mac)
    if normalized is None:
        raise ValueError(f"doorman: unusable MAC {mac!r}")
    mac_bytes = bytes.fromhex(normalized)
    return (b"\xff" * 6) + (mac_bytes * 16)


# ---------------------------------------------------------------------------
# Default side-effecting primitives (the seams tests replace)
# ---------------------------------------------------------------------------

def tcp_probe(host: str, port: int, timeout: float) -> bool:
    """One short TCP connect. True when something is listening on host:port."""
    try:
        with socket.create_connection((host, int(port)), timeout=float(timeout)):
            return True
    except (OSError, ValueError):
        return False


def send_magic_packet(mac: str, broadcast: str, wol_port: int) -> int:
    """Broadcast one magic packet; returns the byte count actually sent."""
    packet = build_magic_packet(mac)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.sendto(packet, (broadcast, int(wol_port)))
    finally:
        sock.close()
    return len(packet)


# ---------------------------------------------------------------------------
# Keep-alive engine: one scheduler thread, per-host holds, hook-driven release
# ---------------------------------------------------------------------------

class KeepaliveEngine:
    """Per-host keep-alive holds with a single scheduler thread.

    Semantics (v0.2.0):

    * ``touch(row)``  — arm or refresh a hold for this host (hook: every ``pre_*``);
      the cap and the packet clock restart from the touch, so a long multi-call turn
      (tool loop / compaction / streaming) keeps the hold alive as long as attempts
      keep arriving.
    * ``release(row)`` — mark a hold released; the scheduler sends ONE final packet
      ``grace`` seconds later, then deletes the hold. Called by ``post_api_request``
      (success), ``post_auxiliary_call``, and ``api_request_error`` when the payload
      marks the failure terminal.
    * Cap: if now - hold_start >= max_seconds, the hold expires with no further
      packets regardless of release state. Stand-in for a release hook that never
      arrives (provider failover mid-attempt, a core hiccup, a turn dropped whole).

    Injection seams (``clock`` / ``sender``) exist so tests can drive the engine
    with a fake time source and a counting sender; production passes None and gets
    the real ones. The scheduler thread is a daemon: it dies with the agent process.
    """

    def __init__(
        self,
        *,
        sender: Optional[Callable[[str, str, int], int]] = None,
        clock: Optional[Callable[[], float]] = None,
        tick: float = KEEPALIVE_TICK,
        defaults: Optional[dict] = None,
    ) -> None:
        self._sender = sender or send_magic_packet
        self._clock = clock or time.monotonic
        self._tick = float(tick)
        self._defaults = defaults or {}
        self._holds: dict[tuple[str, int], dict] = {}
        self._lock = threading.Lock()
        self._thread: Optional[threading.Thread] = None

    # -- public API (called from hook callbacks; never raises) ----------------

    def touch(self, row: Mapping[str, Any], *, logger: Optional[logging.Logger] = None) -> None:
        """Arm/refresh the hold for this row's host:port."""
        log = logger or LOG
        try:
            interval = _coerce_float(
                row.get("keepalive_interval_seconds"),
                float(self._defaults.get("interval", DEFAULT_KEEPALIVE_INTERVAL)),
                minimum=0.0,
            )
            if interval is None or interval <= 0:
                return  # per-row opt-out: interval: 0 means "no keep-alive for this box"
            mac = normalize_mac(row.get("mac"))
            if not mac:
                return
            key = (row.get("host") or "", int(row.get("port") or 0))
            now = self._clock()
            with self._lock:
                hold = self._holds.get(key)
                if hold is None:
                    hold = {
                        "name": row.get("name"),
                        "host": key[0],
                        "port": key[1],
                        "mac": mac,
                        "broadcast": row.get("broadcast") or "255.255.255.255",
                        "wol_port": int(row.get("wol_port") or DEFAULT_WOL_PORT),
                        "interval": float(interval),
                        "max_seconds": _coerce_float(
                            row.get("keepalive_max_seconds"),
                            float(self._defaults.get("max", DEFAULT_KEEPALIVE_MAX)),
                            minimum=1.0,
                        ),
                        "grace": _coerce_float(
                            row.get("keepalive_grace_seconds"),
                            float(self._defaults.get("grace", DEFAULT_KEEPALIVE_GRACE)),
                            minimum=0.0,
                        ),
                        "started": now,
                        "last_fire": now,
                        "released_at": None,
                        "final_sent": False,
                        "packets": 0,
                        "touches": 1,
                    }
                    self._holds[key] = hold
                    log.debug("doorman: [keepalive] armed %s:%s interval=%.0fs max=%.0fs grace=%.0fs",
                              key[0], key[1], hold["interval"], hold["max_seconds"], hold["grace"])
                else:
                    hold["touches"] += 1
                    hold["started"] = now           # cap restarts with activity
                    hold["last_fire"] = now         # next packet scheduled a full interval out
                    hold["released_at"] = None      # a retry re-arms a released hold
                self._ensure_thread_locked()
        except Exception:
            log.warning("doorman: [keepalive] touch failed; continuing", exc_info=True)

    def release(self, row: Mapping[str, Any], *, reason: str = "",
                logger: Optional[logging.Logger] = None) -> None:
        """Mark the hold released: one final packet after ``grace``, then done."""
        log = logger or LOG
        try:
            key = (row.get("host") or "", int(row.get("port") or 0))
            now = self._clock()
            with self._lock:
                hold = self._holds.get(key)
                if hold is not None and hold["released_at"] is None:
                    hold["released_at"] = now
                    log.debug("doorman: [keepalive] released %s:%s (%s) — final packet in %.0fs",
                              key[0], key[1], reason or "?", hold["grace"])
        except Exception:
            log.warning("doorman: [keepalive] release failed; continuing", exc_info=True)

    def release_all(self, *, reason: str = "") -> int:
        """Release every live hold at once (v0.2.2). Used by the ``agent_loop_stopped``
        hook: when the turn is cancelled (/stop, /new fast path) every open hold
        belonged to an attempt that is never coming back, so ghost packets must not
        keep waking a box nobody is talking to. Returns the number of holds released."""
        log = LOG
        released = 0
        try:
            now = self._clock()
            with self._lock:
                for hold in self._holds.values():
                    if hold.get("released_at") is None:
                        hold["released_at"] = now
                        released += 1
            if released:
                log.debug("doorman: [keepalive] released %d hold(s) (%s) — final packet per grace",
                          released, reason or "?")
        except Exception:
            log.warning("doorman: [keepalive] release_all failed; continuing", exc_info=True)
        return released

    # -- internals ------------------------------------------------------------

    def _ensure_thread_locked(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._thread = threading.Thread(
            target=self._run,
            name="doorman-keepalive",
            daemon=True,
        )
        self._thread.start()

    def _run(self) -> None:
        # One scheduler loop for every hold across every mapped host. Fail-open:
        # an exception in one hold's tick is suppressed, the loop keeps scanning.
        while True:
            now = self._clock()
            done: list[tuple[str, int]] = []
            jobs: list[dict] = []
            caps: list[tuple[str, dict]] = []
            with self._lock:
                expired = [key for key, hold in self._holds.items()
                           if (now - hold["started"]) >= hold["max_seconds"]]
                for key in expired:
                    caps.append((key, dict(self._holds.pop(key))))
                for key, hold in self._holds.items():
                    try:
                        if hold["released_at"] is not None:
                            if hold["final_sent"]:
                                done.append(key)
                            elif now - hold["released_at"] >= hold["grace"]:
                                jobs.append(hold)          # fired OUTSIDE the lock
                                hold["final_sent"] = True
                                done.append(key)
                        elif now - hold["last_fire"] >= hold["interval"]:
                            jobs.append(hold)
                            hold["last_fire"] = now
                    except Exception:
                        LOG.warning(
                            "doorman: [keepalive] tick failed for %s:%s; retrying next tick",
                            key[0], key[1], exc_info=True,
                        )
                for key in done:
                    self._holds.pop(key, None)
            # Work happens with the lock RELEASED — the UDP send is slow (socket dial
            # + broadcast) and must never hold up touch()/release() on the hook thread.
            for _, info in caps:
                LOG.warning(
                    "doorman: [keepalive] cap hit %s:%s after %.0fs (%d packet(s), "
                    "%d touch(es)) — no release hook arrived; packets stop here",
                    info["host"], info["port"], now - info["started"],
                    info["packets"], info["touches"],
                )
            for hold in jobs:
                try:
                    self._fire_nolock(hold)
                except Exception:
                    LOG.warning(
                        "doorman: [keepalive] tick failed for %s:%s; retrying next tick",
                        hold["host"], hold["port"], exc_info=True,
                    )
            self._sleep(self._tick)

    def _sleep(self, seconds: float) -> None:
        try:
            time.sleep(max(0.0, float(seconds)))
        except Exception:
            pass

    def _fire_nolock(self, hold: Mapping[str, Any]) -> None:
        """Send one packet and bump the counter (caller must NOT hold the lock)."""
        sent = self._sender(hold["mac"], hold["broadcast"], hold["wol_port"])
        with self._lock:
            # the hold may have been touched/released concurrently; only bump if
            # it is still the same hold object we scheduled — cheap re-check by key
            live = self._holds.get((hold["host"], hold["port"]))
            if live is hold:
                live["packets"] += 1
        LOG.debug("doorman: [keepalive] packet sent to %s:%s (%d bytes)",
                  hold["host"], hold["port"], sent)

    def active(self) -> int:
        """Number of live holds (test/inspection seam; production never calls this)."""
        with self._lock:
            return len(self._holds)

    def holds_snapshot(self) -> dict:
        """Shallow copy of live holds (test/inspection seam)."""
        with self._lock:
            return {k: dict(v) for k, v in self._holds.items()}


# Module-level engine (one per agent process; thread dies with the process).
_GLOBAL_DEFAULTS: dict[str, float] = {}
_ENGINE = KeepaliveEngine(defaults=_GLOBAL_DEFAULTS)


def _remember_defaults(rows: Iterable[dict]) -> None:
    """Pull the keepalive keys out of a row named ``defaults`` into module state.

    The engine reads the dict by reference, so this updates defaults live on the
    next hook fire without restarting anything.
    """
    for row in rows:
        if (row.get("name") or "").strip().lower() != "defaults":
            continue
        _GLOBAL_DEFAULTS.clear()
        if row.get("keepalive_interval_seconds") is not None:
            _GLOBAL_DEFAULTS["interval"] = _coerce_float(
                row.get("keepalive_interval_seconds"), DEFAULT_KEEPALIVE_INTERVAL)
        if row.get("keepalive_max_seconds") is not None:
            _GLOBAL_DEFAULTS["max"] = _coerce_float(
                row.get("keepalive_max_seconds"), DEFAULT_KEEPALIVE_MAX)
        if row.get("keepalive_grace_seconds") is not None:
            _GLOBAL_DEFAULTS["grace"] = _coerce_float(
                row.get("keepalive_grace_seconds"), DEFAULT_KEEPALIVE_GRACE)


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

def _coerce_float(value: Any, default: float, *, minimum: float = 0.0) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if out >= minimum else default


def _coerce_int(value: Any, default: Optional[int] = None,
                *, low: int = 1, high: int = 65535) -> Optional[int]:
    try:
        out = int(value)
    except (TypeError, ValueError):
        return default
    return out if low <= out <= high else default


_SCOPES = ("base_url", "provider", "model")


def split_match(raw: Any) -> tuple[str, str]:
    """``match`` value -> (scope, pattern); scope is ``base_url``/``provider``/``model``.

    Accepts ``"provider:foo"``, ``"model:*qwen*"``, ``"base_url:*gpu-host*"``, a bare
    ``"foo"`` (provider scope), or a single-key mapping ``{provider: foo}`` /
    ``{model: foo}`` / ``{base_url: foo}``. Unparseable input yields an empty
    pattern, which never matches.
    """
    if isinstance(raw, Mapping):
        for scope in _SCOPES:
            value = raw.get(scope)
            if isinstance(value, str) and value.strip():
                return scope, value.strip()
        return "provider", ""
    if not isinstance(raw, str):
        return "provider", ""
    text = raw.strip()
    lowered = text.lower()
    for scope in _SCOPES:
        for sep in (":", "="):
            prefix = scope + sep
            if lowered.startswith(prefix):
                return scope, text[len(prefix):].strip()
    return "provider", text


def parse_row(raw: Any) -> Optional[dict]:
    """Validate one ``provider_wol`` row (named-flat entry, list entry, or fragment).

    None (with a warning) when unusable.
    """
    if not isinstance(raw, Mapping):
        LOG.warning("doorman: ignoring row that is not a mapping (%r)", type(raw).__name__)
        return None
    scope, pattern = split_match(raw.get("match"))
    if not pattern:
        LOG.warning("doorman: ignoring row with empty/unusable match (%r)", raw.get("match"))
        return None
    host = raw.get("host")
    if not isinstance(host, str) or not host.strip():
        LOG.warning("doorman: ignoring row for %s %r — missing host", scope, pattern)
        return None
    mac = normalize_mac(raw.get("mac"))
    if mac is None:
        LOG.warning("doorman: ignoring row for host %s — missing/unusable mac", host.strip())
        return None
    port = _coerce_int(raw.get("port"))
    if port is None:
        # The inference port is configuration, never a plugin-side constant.
        LOG.warning("doorman: ignoring row for host %s — missing/invalid port", host.strip())
        return None
    broadcast = raw.get("broadcast")
    if not isinstance(broadcast, str) or not broadcast.strip():
        broadcast = "255.255.255.255"
    name = raw.get("name")
    if not isinstance(name, str) or not name.strip():
        name = None
    return {
        "name": name,
        "scope": scope,
        "pattern": pattern,
        "host": host.strip(),
        "mac": mac,
        "broadcast": broadcast.strip(),
        "port": port,
        "wol_port": _coerce_int(raw.get("wol_port"), DEFAULT_WOL_PORT),
        "max_wait_seconds": _coerce_float(raw.get("max_wait_seconds"), DEFAULT_MAX_WAIT),
        "cooldown_seconds": _coerce_float(raw.get("cooldown_seconds"), DEFAULT_COOLDOWN),
        "probe_timeout": _coerce_float(raw.get("probe_timeout"), DEFAULT_PROBE_TIMEOUT, minimum=0.05),
        "poll_interval": _coerce_float(raw.get("poll_interval"), DEFAULT_POLL_INTERVAL, minimum=0.05),
        "keepalive_interval_seconds": _coerce_float(
            raw.get("keepalive_interval_seconds"), DEFAULT_KEEPALIVE_INTERVAL, minimum=0.0),
        "keepalive_max_seconds": _coerce_float(
            raw.get("keepalive_max_seconds"), DEFAULT_KEEPALIVE_MAX, minimum=1.0),
        "keepalive_grace_seconds": _coerce_float(
            raw.get("keepalive_grace_seconds"), DEFAULT_KEEPALIVE_GRACE, minimum=0.0),
    }


def _merge_fragmented(raw_rows):
    """Merge a list of single-key mappings into one row.

    YAML users often put each field on its own ``-`` line; that parses as a
    list of one-key dicts rather than one multi-key entry. If every item is a
    single-key mapping, treat the whole list as one fragmented row. (A valid
    row always needs several keys, so all-single-key can only mean fragments.)
    """
    if raw_rows and all(isinstance(r, Mapping) and len(r) == 1 for r in raw_rows):
        merged: dict = {}
        for fragment in raw_rows:
            merged.update(fragment)
        return [merged]
    return list(raw_rows)


def parse_rows(block: Any) -> list[dict]:
    """``provider_wol`` config block -> validated rows.

    Three accepted shapes. The flat single-row form (bare mapping whose
    values are scalars), the named-targets form (bare mapping whose values
    are themselves mappings — the label becomes the row's ``name``; the
    special label ``defaults`` overrides keep-alive defaults for all rows),
    or a bare list of entries. A list of single-key mappings (one field
    per line) is merged into one row first; otherwise each item must be a
    complete entry mapping.
    """
    if isinstance(block, Mapping):
        if any(isinstance(v, Mapping) for v in block.values()):
            # Named-targets form: {label: {field: value...}}. The label rides
            # along as the row's ``name`` for status display and log lines.
            raw_rows: list[Any] = [
                {**fields, "name": label}
                if fields.get("name") is None
                else fields
                for label, fields in block.items()
                if isinstance(fields, Mapping)
            ]
        else:
            raw_rows = [block]
    elif isinstance(block, (list, tuple)):
        raw_rows = _merge_fragmented(list(block))
    else:
        if block is not None:
            LOG.warning(
                "doorman: config must be a bare mapping (flat single-row or "
                "named-targets form) or a list of entries, got %s",
                type(block).__name__,
            )
        return []
    rows: list[dict] = []
    for raw in raw_rows:
        # The ``defaults`` label is a pseudo-row: keepalive keys become module
        # defaults and it is not itself a parseable target (no match/host).
        if (raw.get("name") or "").strip().lower() == "defaults":
            _remember_defaults([raw])
            continue
        row = parse_row(raw)
        if row is not None:
            rows.append(row)
    return rows


def load_rows() -> list[dict]:
    """Re-read ``provider_wol`` from config.yaml on every call (hot reload; no restart).

    Mirrors the agent-hud pattern: read-only, fail-open, never raises.
    """
    try:
        from hermes_cli.config import cfg_get, load_config

        cfg = load_config() or {}
        block = cfg_get(cfg, CONFIG_BLOCK, default=None)
        return parse_rows(block)
    except Exception as exc:  # config import problem must never touch the turn
        LOG.debug("doorman: config read failed (%s); no rows", exc)
        return []


def _hook_callback_timeout() -> float:
    """Effective ``plugins.hook_callback_timeout`` (0/negative = threaded path disabled)."""
    # Read the documented config key directly (public config surface, plugin-side);
    # no private core imports — catalog rule 9 (public surfaces only).
    try:
        from hermes_cli.config import cfg_get, load_config

        plugins_cfg = cfg_get(load_config() or {}, "plugins", default={}) or {}
        value = plugins_cfg.get("hook_callback_timeout")
        if value is None:
            return DEFAULT_HOOK_TIMEOUT
        return float(value)
    except Exception:
        return DEFAULT_HOOK_TIMEOUT


def wait_budget() -> Optional[float]:
    """Seconds this callback may spend waiting, or None when no cap applies.

    ``pre_api_request`` is in ``_HOOK_TIMEOUT_BOUNDED_HOOKS``
    (hermes_cli/plugins_dispatch.py:42-44) and is abandoned after
    ``plugins.hook_callback_timeout`` (default 30s, same file :158) — a wait that
    overruns that budget is pointless, so the default 30s max_wait is clamped to
    ``hook_callback_timeout - HOOK_TIMEOUT_MARGIN``. A timeout <= 0 disables the
    threaded path entirely (hermes_cli/plugins.py:1126-1127), so nothing to clamp.
    """
    timeout = _hook_callback_timeout()
    if timeout <= 0:
        return None
    return max(0.0, timeout - HOOK_TIMEOUT_MARGIN)


# ---------------------------------------------------------------------------
# Mapping resolution
# ---------------------------------------------------------------------------

def _pattern_matches(pattern: str, value: str) -> bool:
    pattern = pattern.strip().lower()
    value = (value or "").strip().lower()
    if not pattern or not value:
        return False
    if "*" in pattern or "?" in pattern:
        return fnmatch.fnmatchcase(value, pattern)
    return value == pattern


def resolve_rows(rows: Iterable[dict], provider: Optional[str], model: Optional[str],
                 base_url: Optional[str] = None) -> list[dict]:
    """Rows whose match applies to this attempt (provider scope OR model scope).

    A row matching both scopes is returned once. Order follows config order.
    """
    matched: list[dict] = []
    seen: set[int] = set()
    for row in rows:
        hit = False
        if row["scope"] == "provider":
            hit = _pattern_matches(row["pattern"], provider or "")
        elif row["scope"] == "base_url":
            hit = _pattern_matches(row["pattern"], (base_url or "").rstrip("/"))
        else:
            hit = _pattern_matches(row["pattern"], model or "")
        if hit and id(row) not in seen:
            seen.add(id(row))
            matched.append(row)
    return matched


# ---------------------------------------------------------------------------
# One host: probe -> wake -> bounded wait
# ---------------------------------------------------------------------------

def ensure_awake(
    row: Mapping[str, Any],
    *,
    probe: Optional[Callable[[str, int, float], bool]] = None,
    sender: Optional[Callable[[str, str, int], int]] = None,
    clock: Optional[Callable[[], float]] = None,
    sleep: Optional[Callable[[float], None]] = None,
    state: Optional[dict] = None,
    budget: Optional[float] = None,
    logger: Optional[logging.Logger] = None,
) -> dict:
    """Make one mapped host reachable, or give up quietly.

    Returns an outcome dict — never raises, never blocks past its deadline:
        status: "up" | "woke" | "cooldown" | "timeout" | "error"
        plus host, port, mac, elapsed, probes, packets
    """
    probe = probe or tcp_probe
    sender = sender or send_magic_packet
    clock = clock or time.monotonic
    sleep = sleep or time.sleep
    log = logger or LOG
    wake_state = _WAKE_STATE if state is None else state

    host = row["host"]
    port = int(row["port"])
    mac = row["mac"]
    probe_timeout = float(row.get("probe_timeout", DEFAULT_PROBE_TIMEOUT))
    poll_interval = float(row.get("poll_interval", DEFAULT_POLL_INTERVAL))
    max_wait = float(row.get("max_wait_seconds", DEFAULT_MAX_WAIT))
    cooldown = float(row.get("cooldown_seconds", DEFAULT_COOLDOWN))

    started = clock()
    outcome = {"host": host, "port": port, "mac": mac, "status": "error",
               "elapsed": 0.0, "probes": 0, "packets": 0}

    # One short reachability probe. Reachable -> nothing to do.
    outcome["probes"] += 1
    if probe(host, port, probe_timeout):
        outcome["status"] = "up"
        outcome["elapsed"] = clock() - started
        log.debug("doorman: %s:%s reachable (%.3fs probe); no wake needed",
                  host, port, outcome["elapsed"])
        return outcome

    now = clock()
    last_wake = wake_state.get(host)
    in_flight = last_wake is not None and (now - last_wake) < cooldown

    if in_flight:
        # Anti-spam: a packet for this host is already in flight. Don't resend; give the
        # existing wake cycle only the balance of its own budget.
        outcome["status"] = "cooldown"
        deadline = float(last_wake) + max_wait
        log.debug("doorman: %s:%s unreachable but woken %.1fs ago (< cooldown %.0fs); "
                  "not re-spamming, waiting out %.1fs of the existing cycle",
                  host, port, now - float(last_wake), cooldown, max(0.0, deadline - now))
    else:
        try:
            sent_bytes = sender(mac, str(row.get("broadcast") or "255.255.255.255"),
                                int(row.get("wol_port", DEFAULT_WOL_PORT)))
            outcome["packets"] = 1
            wake_state[host] = now
            deadline = now + max_wait
            log.debug("doorman: %s:%s unreachable; sent %d-byte magic packet for %s to %s:%s",
                      host, port, sent_bytes, mac, row.get("broadcast"),
                      row.get("wol_port", DEFAULT_WOL_PORT))
        except Exception as exc:
            outcome["elapsed"] = clock() - started
            log.warning("doorman: %s:%s unreachable and the magic packet could not be sent "
                        "(%s: %s); proceeding without waking", host, port, type(exc).__name__, exc)
            return outcome

    if budget is not None:
        deadline = min(deadline, now + max(0.0, float(budget)))

    # Bounded wait-for-port loop.
    polls = 0
    while True:
        current = clock()
        if current >= deadline:
            break
        sleep(min(poll_interval, deadline - current))
        outcome["probes"] += 1
        polls += 1
        if probe(host, port, probe_timeout):
            outcome["status"] = "woke"
            outcome["elapsed"] = clock() - started
            log.debug("doorman: %s:%s came up after %.1fs (%d poll(s)); proceeding",
                      host, port, outcome["elapsed"], polls)
            return outcome

    outcome["elapsed"] = clock() - started
    if outcome["status"] != "cooldown":
        outcome["status"] = "timeout"
    log.warning(
        "doorman: WAKE FAILED host=%s port=%s mac=%s elapsed=%.1fs probe=TCP-connect"
        "(timeout=%.2fs) every %.1fs sent=%d magic-packet(s)[6xFF+MAC*16] to %s:%s cooldown=%.0fs "
        "result=%s — target never accepted a connection; proceeding without a live backend",
        host, port, mac, outcome["elapsed"], probe_timeout, poll_interval, outcome["packets"],
        row.get("broadcast") or "255.255.255.255", row.get("wol_port", DEFAULT_WOL_PORT),
        cooldown, "cooldown-suppressed wake still not up" if outcome["status"] == "cooldown"
        else "no reply after wake",
    )
    return outcome


# ---------------------------------------------------------------------------
# Hook callbacks
# ---------------------------------------------------------------------------

def handle_pre_api_request(
    *, provider: Optional[str] = None, model: Optional[str] = None,
    base_url: Optional[str] = None, **kwargs: Any,
) -> None:
    """Wake every mapped host this attempt targets, then arm/refresh its keep-alive.

    Fail-open; returns None always. Payload (agent/turn_api_request.py:63-88)
    supplies provider, model, base_url — the earliest point where per-attempt
    provider identity exists. The keep-alive is armed BEFORE the wake attempt so a
    post-hook arriving for a retry/second call finds it; every refresh extends the
    cap, so long multi-call turns stay covered end to end.
    """
    try:
        rows = load_rows()
    except Exception as exc:
        LOG.warning("doorman: [diag] fired provider=%r model=%r — row load failed (%s)",
                    provider, model, exc)
        return
    if not rows:
        LOG.warning("doorman: [diag] fired provider=%r model=%r base_url=%r — no rows loaded",
                    provider, model, base_url)
        return
    targets = resolve_rows(rows, provider, model, base_url)
    if not targets:
        LOG.debug("doorman: [diag] fired provider=%r model=%r base_url=%r — rows=%d matched=0",
                  provider, model, base_url, len(rows))
        return
    LOG.debug("doorman: provider=%r model=%r base_url=%r -> %d mapped host(s): %s",
              provider, model, base_url, len(targets),
              ", ".join(f"{r['host']}:{r['port']}" for r in targets))
    budget = wait_budget()
    for row in targets:
        try:
            _ENGINE.touch(row)
        except Exception as exc:  # engine failure must still leave the wake running
            LOG.warning("doorman: keep-alive arm for %s:%s failed (%s: %s); proceeding",
                        row.get("host"), row.get("port"), type(exc).__name__, exc)
        try:
            outcome = ensure_awake(row, budget=budget)
        except Exception as exc:  # never raise into the turn
            LOG.warning("doorman: wake attempt for %s:%s failed (%s: %s); proceeding",
                        row.get("host"), row.get("port"), type(exc).__name__, exc)
            continue
        # Receipt on EVERY mapped invocation outcome — matched path must be visible; a
        # successful 'up' still names which host the attempt touched (the audit trail
        # the evening's tests taught us to want; unmatched=quiet stays true).
        lane = ("aux:" + str(kwargs.get("aux_task") or "?")) if "aux_task" in kwargs else "main"
        LOG.warning("doorman: [receipt] %s provider=%r model=%r -> %s:%s status=%s elapsed=%.1fs",
                    lane, provider, model, row["host"], row["port"],
                    outcome.get("status"), float(outcome.get("elapsed") or 0.0))


def handle_post_api_request(
    *, provider: Optional[str] = None, model: Optional[str] = None,
    base_url: Optional[str] = None, **kwargs: Any,
) -> None:
    """Attempt ended successfully — release the matching hold (final packet after grace)."""
    _release_matching(provider=provider, model=model, base_url=base_url,
                      reason="post_api_request")


def handle_post_auxiliary_call(
    *, provider: Optional[str] = None, model: Optional[str] = None,
    base_url: Optional[str] = None, **kwargs: Any,
) -> None:
    """Aux attempt finished — release the matching hold (final packet after grace)."""
    _release_matching(provider=provider, model=model, base_url=base_url,
                      reason="post_auxiliary_call")


def handle_api_request_error(
    *, provider: Optional[str] = None, model: Optional[str] = None,
    base_url: Optional[str] = None, retryable: Optional[bool] = None,
    retry_count: Optional[int] = None, max_retries: Optional[int] = None,
    reason: Optional[str] = None, **kwargs: Any,
) -> None:
    """Error path — holds stay for a retry the gateway will fire; release otherwise.

    Terminal means ``not retryable`` or ``retry_count >= max_retries`` (payload:
    agent/api_request_hooks.py:173-197). On a terminal error the hold releases with
    the grace packet; on a retryable one it stays and the retry's own pre-hook keeps
    refreshing it — so packets continue through the backoff, per the design brief.
    """
    terminal = (not retryable) or (
        isinstance(retry_count, int) and isinstance(max_retries, int)
        and max_retries >= 0 and retry_count >= max_retries
    )
    if terminal:
        _release_matching(provider=provider, model=model, base_url=base_url,
                          reason=f"api_request_error:{reason or 'terminal'}")
    else:
        LOG.debug("doorman: keep-alive held through retryable error (%s, %s/%s)",
                  reason, retry_count, max_retries)


def handle_agent_loop_stopped(**_kwargs: Any) -> None:
    """v0.2.2: the turn is gone (/stop, /new running-agent fast path). Every live
    hold belonged to that attempt — release them all so a manually-slept box is
    not knocked back up by ghost packets for a request nobody is waiting on.
    Fail-open like every other path in this plugin.
    """
    try:
        _ENGINE.release_all(reason="agent_loop_stopped")
    except Exception:
        LOG.warning("doorman: agent_loop_stopped release failed; continuing", exc_info=True)


def _release_matching(
    *, provider: Optional[str], model: Optional[str], base_url: Optional[str],
    reason: str, **kwargs: Any,
) -> None:
    try:
        rows = load_rows()
        if not rows:
            return
        targets = resolve_rows(rows, provider, model, base_url)
        for row in targets:
            _ENGINE.release(row, reason=reason)
    except Exception:
        LOG.warning("doorman: release matching failed; continuing", exc_info=True)


def register(ctx: Any) -> None:
    """Plugin entry point: six observer hooks, no tools, no commands, no core contact."""
    ctx.register_hook(HOOK_NAME, handle_pre_api_request)
    # Auxiliary funnel (agent/auxiliary_hooks.py): vision/titling/compression/... ride a
    # separate hook pair with the same payload shape — same wake path, same budget clamp.
    ctx.register_hook("pre_auxiliary_call", handle_pre_api_request)
    # Lifecycle companions (v0.2.0): success/finish on both funnels + the error hook
    # carry the same provider/model/base_url identity, so keep-alive release matching
    # is the same resolver as wake matching. Old cores ignore unknown hooks
    # (cosmetic unknown-hook warning, established behavior).
    ctx.register_hook("post_api_request", handle_post_api_request)
    ctx.register_hook("post_auxiliary_call", handle_post_auxiliary_call)
    ctx.register_hook("api_request_error", handle_api_request_error)
    # v0.2.2: turn-cancellation. /stop and the /new fast path fire this (payload:
    # session_key/platform/reason) — the hold's release hooks never fire for a
    # cancelled turn, so this is the only signal the attempt is gone.
    ctx.register_hook("agent_loop_stopped", handle_agent_loop_stopped)
    LOG.warning("doorman: [diag] plugin loaded — registered hooks %s + pre_auxiliary_call "
                "+ post_api_request + post_auxiliary_call + api_request_error "
                "+ agent_loop_stopped", HOOK_NAME)

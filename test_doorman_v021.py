#!/usr/bin/env python3
"""v0.2.1 tests for doorman keep-alive + config shapes.

Run: python3 test_doorman_v021.py
Pure stdlib + the plugin under test (imported by path so nothing else loads).
"""
import sys
import types
from pathlib import Path

# import the plugin from THIS directory (the suite runs against its own checkout)
PLUGIN = Path(__file__).resolve().parent
sys.path.insert(0, str(PLUGIN))
import importlib.util
spec = importlib.util.spec_from_file_location("doorman_v021",
                                              PLUGIN / "__init__.py")
pw = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pw)

PASS = 0
FAIL = 0

def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  ok  {name}")
    else:
        FAIL += 1
        print(f"FAIL  {name} {detail}")


# ---------------------------------------------------------------------------
# 1. config shapes
# ---------------------------------------------------------------------------

print("== config shapes ==")
named = {
    "defaults": {"keepalive_interval_seconds": 60, "keepalive_max_seconds": 100,
                 "keepalive_grace_seconds": 5},
    "gpu-host": {"match": "base_url:*gpu-host*", "host": "10.0.20.10",
             "mac": "aa:bb:cc:dd:ee:01", "port": 1234},
    "other": {"match": "provider:lmstudio", "host": "10.0.20.20",
              "mac": "00:11:22:33:44:55", "port": 1234,
              "keepalive_interval_seconds": 0},
}
rows = pw.parse_rows(named)
check("named form -> 2 target rows (defaults is not a target)", len(rows) == 2, str(len(rows)))
byname = {r["name"]: r for r in rows if r["name"] != "defaults"}
check("defaults row absorbed defaults", pw._GLOBAL_DEFAULTS.get("interval") == 60.0,
      str(pw._GLOBAL_DEFAULTS))
check("named row keeps name", byname.get("gpu-host", {}).get("name") == "gpu-host")
check("per-row interval 0 preserved", byname["other"]["keepalive_interval_seconds"] == 0.0)
check("row dict itself carries module default (defaults apply at engine level)",
      byname["gpu-host"]["keepalive_interval_seconds"] == 120.0)
# engine-level defaults proof: a row WITHOUT per-row keys gets the defaults interval
row_no_keys = {"name": "gpu-host-bare", "host": "10.0.20.90", "port": 1234,
               "mac": "aa:bb:cc:dd:ee:01"}
clock_def = (lambda: 500.0)
eng_def = pw.KeepaliveEngine(sender=lambda *a: 102, clock=clock_def,
                             defaults=dict(pw._GLOBAL_DEFAULTS))
eng_def.touch(row_no_keys)
snapd = eng_def.holds_snapshot()[("10.0.20.90", 1234)]
check("engine honors defaults row for interval", snapd["interval"] == 60.0)
check("engine honors defaults row for max", snapd["max_seconds"] == 100.0)
check("engine honors defaults row for grace", snapd["grace"] == 5.0)
eng_def.release(row_no_keys)  # tidy: stops the thread's ticking for this test engine

flat = {"match": "provider:lmstudio", "host": "10.0.20.10",
        "mac": "aa:bb:cc:dd:ee:01", "port": 1234}
rows2 = pw.parse_rows(flat)
check("flat form still parses", len(rows2) == 1 and rows2[0]["host"] == "10.0.20.10")

listed = [{"match": "base_url:*gpu-host*", "host": "10.0.20.10",
           "mac": "aa:bb:cc:dd:ee:01", "port": 1234}]
rows3 = pw.parse_rows(listed)
check("list form still parses", len(rows3) == 1)

frag = [{"host": "10.0.20.10"}, {"mac": "aa:bb:cc:dd:ee:01"},
        {"match": "base_url:*gpu-host*"}, {"port": 1234}]
rows4 = pw.parse_rows(frag)
check("fragmented list merges to one row", len(rows4) == 1 and rows4[0]["port"] == 1234)

# ---------------------------------------------------------------------------
# 2. engine: fake clock + counting sender
# ---------------------------------------------------------------------------

print("== engine ==")


class FakeClock:
    def __init__(self):
        self.t = 1000.0
    def __call__(self):
        return self.t


sent = []
def counting_sender(mac, broadcast, wol_port):
    sent.append((mac, broadcast, wol_port))
    return 102


row = {"name": "gpu-host", "host": "10.0.20.10", "port": 1234,
       "mac": "aa:bb:cc:dd:ee:01", "broadcast": "10.0.20.255",
       "keepalive_interval_seconds": 10, "keepalive_max_seconds": 100,
       "keepalive_grace_seconds": 5, "wol_port": 9}

clock = FakeClock()
eng = pw.KeepaliveEngine(sender=counting_sender, clock=clock)

# arm: no immediate fire expected (last_fire == now)
eng.touch(row)
check("arm creates hold", eng.active() == 1)
check("armed hold schedules first packet interval out", sent == [], str(sent))

clock.t += 9.9
eng._run  # noqa: reference only (thread runs in background; drive tick manually below)

# drive one manual tick cycle by calling the internal loop body via a thread-free path:
# simplest deterministic way: step the clock past interval then run the scheduler body
# for real through the daemon thread is async — instead call an explicit scan helper.
# Fires happen with the lock RELEASED (same shape as the fixed _run).
def scan(engine):
    """Run the engine's scheduler inner work synchronously (mirror of _run body)."""
    now = engine._clock()
    done = []
    jobs = []
    with engine._lock:
        expired = [k for k, h in engine._holds.items()
                   if (now - h["started"]) >= h["max_seconds"]]
        for k in expired:
            engine._holds.pop(k)
        for k, h in list(engine._holds.items()):
            if h["released_at"] is not None:
                if h["final_sent"]:
                    done.append(k)
                elif now - h["released_at"] >= h["grace"]:
                    jobs.append(h)          # fired outside the lock, like _run does
                    h["final_sent"] = True
                    done.append(k)
            elif now - h["last_fire"] >= h["interval"]:
                jobs.append(h)
                h["last_fire"] = now
        for k in done:
            engine._holds.pop(k, None)
    for h in jobs:
        engine._fire_nolock(h)
    return done


scan(eng)
check("no packet before interval elapses", sent == [])

clock.t += 10.0
scan(eng)
check("packet fired at interval", len(sent) == 1, str(sent))

clock.t += 10.0
scan(eng)
check("second packet at next interval", len(sent) == 2)

# release -> grace -> final packet -> closed
eng.release(row, reason="test")
clock.t += 4.0
scan(eng)
check("no final packet before grace elapses", len(sent) == 2)

clock.t += 1.1
scan(eng)
check("grace final packet sent", len(sent) == 3)
check("hold closed after final", eng.active() == 0, str(eng.holds_snapshot()))

# refresh semantics: released hold re-armed by a later touch (retry path)
eng.touch(row)
check("retry touch re-arms released hold", eng.active() == 1)
snap = eng.holds_snapshot()
key = ("10.0.20.10", 1234)
check("re-armed hold released_at cleared", snap[key]["released_at"] is None)

# cap: leave hold un-touched past max
clock.t += 200.0
scan(eng)
check("leak cap expires the hold", eng.active() == 0)

# interval:0 rows are never armed
row0 = dict(row, keepalive_interval_seconds=0)
eng.touch(row0)
check("interval:0 disables keep-alive", eng.active() == 0)
scan(eng)
check("no packets for disabled row", len(sent) == 3, str(sent))

# ---------------------------------------------------------------------------
# 3. error-hook terminal logic (module level, no engine dependency)
# ---------------------------------------------------------------------------

print("== error hook ==")
# monkeypatch the release resolver to record calls
released = []
orig_resolver = pw._release_matching
pw._release_matching = lambda *, provider, model, base_url, reason, **kw: released.append(reason)

pw.handle_api_request_error(provider="lmstudio", base_url="http://gpu-host:1234/v1",
                            retryable=True, retry_count=0, max_retries=3)
check("retryable error holds", released == [], str(released))

pw.handle_api_request_error(provider="lmstudio", retryable=True, retry_count=3, max_retries=3)
check("retry exhaustion releases", released == ["api_request_error:terminal"], str(released))

pw.handle_api_request_error(provider="lmstudio", retryable=False)
check("non-retryable releases", len(released) == 2)

pw.handle_api_request_error(provider="lmstudio")
check("unknown/absent retry semantic treated as terminal", len(released) == 3)
pw._release_matching = orig_resolver

# ---------------------------------------------------------------------------
# 4. registers five hooks (fake ctx)
# ---------------------------------------------------------------------------

print("== registration ==")
registered = []
class FakeCtx:
    def register_hook(self, name, func):
        registered.append((name, func is not None))

pw.register(FakeCtx())
names = [n for n, _ in registered]
check("six hooks registered (incl. v0.2.2 agent_loop_stopped)",
      names == ["pre_api_request", "pre_auxiliary_call", "post_api_request",
                "post_auxiliary_call", "api_request_error", "agent_loop_stopped"], str(names))
check("all callbacks non-null", all(ok for _, ok in registered))

# ---------------------------------------------------------------------------
# 4b. v0.2.2 agent_loop_stopped releases every live hold
# ---------------------------------------------------------------------------

print("== agent_loop_stopped release ==")
# drive the MODULE engine — the handler releases the global one by design
row_stop = {"name": "stop-test", "host": "10.0.20.77", "port": 1234,
            "mac": "aa:bb:cc:dd:ee:09"}   # synthetic MAC: any packet is a LAN no-op
pw._ENGINE.touch(row_stop)
snap_stop = pw._ENGINE.holds_snapshot().get(("10.0.20.77", 1234), {})
check("hold live before stop", snap_stop.get("released_at") is None, str(snap_stop))
before = len(pw._ENGINE.holds_snapshot())
pw.handle_agent_loop_stopped(session_key="k", platform="telegram", reason="user")
snap_stop = pw._ENGINE.holds_snapshot().get(("10.0.20.77", 1234), {})
check("stop releases the hold (released_at set)",
      snap_stop.get("released_at") is not None, str(snap_stop))
pw.handle_agent_loop_stopped()  # idempotent: already released, must not raise or change
snap2 = pw._ENGINE.holds_snapshot().get(("10.0.20.77", 1234), {})
check("second stop is a no-op on a released hold",
      snap2.get("released_at") == snap_stop.get("released_at"), str(snap2))
check("stop did not create or destroy hold rows",
      len(pw._ENGINE.holds_snapshot()) == before, str(before))

# ---------------------------------------------------------------------------
# 5. release matching is symmetric with wake matching (same resolver)
# ---------------------------------------------------------------------------

print("== symmetric release match ==")
rows5 = pw.parse_rows({"gpu-host": {"match": "base_url:*gpu-host*", "host": "10.0.20.10",
                                "mac": "aa:bb:cc:dd:ee:01", "port": 1234}})
need = "MATCH" if pw.resolve_rows(rows5, "anything", "any-model",
                                  "http://gpu-host:1234/v1") else "NONE"
check("base_url match resolves for release", need == "MATCH")

print(f"\n{PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)

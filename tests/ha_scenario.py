"""
Run by test_v050.py in a fresh interpreter (Redis settings are read at startup):
a primary, a replica and three Sentinels on local ports, PyTaskQ connected
through the Sentinels with REDIS_WAIT_REPLICAS=1. Queues jobs while the
primary is killed, and prints what happened as JSON. Needs `redis-server`.
"""

import asyncio
import json
import os
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

BASE = int(os.environ.get("HA_BASE_PORT", "6411"))
PRIMARY, REPLICA = BASE, BASE + 1
SENTINELS = [BASE + 10, BASE + 11, BASE + 12]
work = tempfile.mkdtemp(prefix="pytaskq-ha-")
procs = {}


def start(name, args):
    procs[name] = subprocess.Popen(["redis-server", *args], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def redis_cli(port, *cmd):
    return subprocess.run(["redis-cli", "-p", str(port), *cmd], capture_output=True, text=True).stdout.strip()


def wait_for(port, timeout=10):
    end = time.time() + timeout
    while time.time() < end:
        if redis_cli(port, "PING") == "PONG":
            return
        time.sleep(0.05)
    raise SystemExit(f"redis on {port} didn't start")


def boot():
    start("primary", ["--port", str(PRIMARY), "--save", "", "--appendonly", "no", "--dir", work])
    wait_for(PRIMARY)
    start("replica", ["--port", str(REPLICA), "--save", "", "--appendonly", "no", "--dir", work,
                      "--replicaof", "127.0.0.1", str(PRIMARY)])
    wait_for(REPLICA)
    for port in SENTINELS:
        conf = os.path.join(work, f"sentinel-{port}.conf")
        with open(conf, "w") as f:
            f.write(f"port {port}\ndir {work}\n"
                    f"sentinel monitor mymaster 127.0.0.1 {PRIMARY} 2\n"
                    f"sentinel down-after-milliseconds mymaster 1000\n"
                    f"sentinel failover-timeout mymaster 5000\n"
                    f"sentinel parallel-syncs mymaster 1\n")
        start(f"sentinel-{port}", [conf, "--sentinel"])
        wait_for(port)
    # the replica is in sync before we start
    end = time.time() + 10
    while "master_link_status:up" not in redis_cli(REPLICA, "INFO", "replication"):
        if time.time() > end:
            raise SystemExit("replica never synced")
        time.sleep(0.1)
    time.sleep(1.5)      # sentinels discover the replica and each other


async def scenario():
    os.environ["REDIS_SENTINELS"] = ",".join(f"127.0.0.1:{p}" for p in SENTINELS)
    os.environ["REDIS_SENTINEL_MASTER"] = "mymaster"
    os.environ["REDIS_WAIT_REPLICAS"] = "1"
    os.environ["REDIS_WAIT_TIMEOUT_MS"] = "300"
    from pytaskq.core import queueing, redis_client
    r = redis_client.r
    out = {"confirmed": [], "errors": 0}

    async def put(i):
        res = await queueing.enqueue(r, queueing.new_payload("job", [i], task_id=f"j{i}"))
        return res

    # An idle worker's 5 s wait through Sentinel must not time out (break test E5).
    try:
        began = time.monotonic()
        out["idle_wait"] = [await r.blpop(["nothing-here"], timeout=5), round(time.monotonic() - began, 1)]
    except Exception as e:   # noqa: BLE001
        out["idle_wait"] = type(e).__name__
    first = await put(0)
    out["first_replicas"] = first.get("replicas")
    out["confirmed"].append("j0")

    # Kill the primary and keep queueing, the way an app would (retrying on errors).
    procs["primary"].kill()
    killed = time.monotonic()
    i, recovered_at = 1, None
    while time.monotonic() - killed < 30:
        try:
            res = await put(i)
            out["confirmed"].append(f"j{i}")
            if recovered_at is None:
                recovered_at = time.monotonic()
            i += 1
            if len(out["confirmed"]) >= 20:
                break
        except Exception as e:   # noqa: BLE001  (ConnectionError / MasterNotFound during the failover)
            out["errors"] += 1
            out["last_error"] = type(e).__name__
            await asyncio.sleep(0.2)
    out["seconds_unavailable"] = round((recovered_at or time.monotonic()) - killed, 1)
    out["new_primary_port"] = int((await asyncio.to_thread(
        redis_cli, SENTINELS[0], "SENTINEL", "get-master-addr-by-name", "mymaster")).split()[-1])
    # Every job confirmed before and after the failover is on the new primary.
    present = [tid for tid in out["confirmed"] if await r.exists(queueing.task_key(tid))]
    out["missing"] = sorted(set(out["confirmed"]) - set(present))
    out["queued_now"] = await r.llen(queueing.queue_key("default"))
    # No replica left now (the old primary is dead): WAIT can't be satisfied, the job is still queued.
    res = await put(999)
    out["replicas_without_replica"] = res.get("replicas")
    out["heartbeat_client_ping"] = await asyncio.to_thread(lambda: redis_client.make_sync_client().ping())
    await r.aclose()
    return out


try:
    boot()
    print(json.dumps(asyncio.run(scenario())))
finally:
    for p in procs.values():
        p.kill()

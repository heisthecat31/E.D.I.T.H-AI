"""Test the relay as three games in one match would: rooms, her balls from a puppet to everyone, the
ball rate limit, and that balls don't use up the other messages' budget.

    python tools/test_relay.py [http://127.0.0.1:8000]

Exit code 1 when a check fails.
"""
import asyncio
import json
import sys
import time

import websockets

URL = (sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8000").rstrip("/").replace("http", "ws", 1) + "/edith/ws"
NAMES = ["Host", "Bob", "Ann"]
FAILED = []


def check(ok, what):
    print(f"   {'PASS' if ok else 'FAIL'}: {what}")
    if not ok:
        FAILED.append(what)


async def texts(ws, secs):
    """Every text message for `secs`."""
    end, got = time.time() + secs, []
    while time.time() < end:
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=max(0.05, end - time.time()))
        except asyncio.TimeoutError:
            break
        if isinstance(raw, str):
            got.append(json.loads(raw))
    return got


async def main():
    socks = {}
    for i, n in enumerate(NAMES):
        socks[n] = await websockets.connect(URL)
        await socks[n].send(json.dumps({"v": 1, "t": "hello", "name": n, "roster": [x for x in NAMES if x != n],
                                        "ai": n == "Host", "build": "test"}))
        await asyncio.sleep(0.05)
    rooms = {n: [m for m in await texts(socks[n], 0.5) if m["t"] == "room"] for n in NAMES}
    check(all(r and r[-1]["host"] == "Host" and len(r[-1]["members"]) == 3 for r in rooms.values()),
          "one room of three, hosted by the player whose companion runs")

    print("\n== a puppet holds one of her balls")
    ball = {"t": "ball", "k": 3, "pos": [1.0, 2.0, 3.0], "vel": [0.5, 0.0, -1.0], "held": 1}
    await socks["Bob"].send(json.dumps(ball))
    got = {n: await texts(socks[n], 0.4) for n in NAMES}
    for n in ("Host", "Ann"):
        b = [m for m in got[n] if m["t"] == "ball"]
        check(b and b[0] == dict(ball, **{"from": "Bob"}), f"{n} gets it, stamped from=Bob")
    check(not any(m["t"] == "ball" for m in got["Bob"]), "not echoed to the sender")
    await socks["Host"].send(json.dumps(dict(ball, k=7, held=0)))
    got = await texts(socks["Bob"], 0.4)
    check(any(m["t"] == "ball" and m["k"] == 7 and m["held"] == 0 and m["from"] == "Host" for m in got),
          "the host's release reaches the others too")

    print("\n== rate limit: 60 balls at once")
    for i in range(60):
        await socks["Ann"].send(json.dumps(dict(ball, k=i % 16)))
    await socks["Ann"].send(json.dumps({"t": "touch", "what": "grab"}))      # (the events budget is apart)
    got = await texts(socks["Host"], 0.6)
    n = sum(1 for m in got if m["t"] == "ball")
    check(n == 30, f"30 of them pass in that second (got {n})")
    check(any(m["t"] == "touch" and m["from"] == "Ann" for m in got), "a touch after them still reaches the host")
    await asyncio.sleep(1.0)
    await socks["Ann"].send(json.dumps(ball))
    got = await texts(socks["Host"], 0.4)
    check(any(m["t"] == "ball" for m in got), "next second, balls pass again")

    print("\n== someone not in the match")
    out = await websockets.connect(URL)
    await out.send(json.dumps({"v": 1, "t": "hello", "name": "Zed", "roster": ["Nobody"], "build": "test"}))
    await texts(out, 0.3)
    await out.send(json.dumps(ball))
    got = await texts(socks["Host"], 0.4)
    check(not any(m["t"] == "ball" and m.get("from") == "Zed" for m in got), "their balls go nowhere")
    for ws in list(socks.values()) + [out]:
        await ws.close()


asyncio.run(main())
print("\n" + (f"{len(FAILED)} FAILED:\n  " + "\n  ".join(FAILED) if FAILED else "all checks passed"))
sys.exit(1 if FAILED else 0)

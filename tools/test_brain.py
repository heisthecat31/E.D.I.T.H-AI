"""Test her brain as a game would: her chat model, self-learning, reports, telling the owner on spawn.

    python tools/test_brain.py [http://127.0.0.1:8000]       (ADMIN_TOKEN in the environment for the
                                                              /stats, /reports and /learned dumps)
Typed lines only (no audio): Whisper and Kokoro can be off (EDITH_STT=0, EDITH_TTS=0).
"""
import asyncio
import json
import os
import sys
import time
import urllib.request

import websockets

HTTP = (sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8000").rstrip("/")
URL = HTTP.replace("http", "ws", 1) + "/edith/brain"
TOKEN = os.environ.get("ADMIN_TOKEN", "")
STATE = "state mode=host order=none her=0,2,0 player=1,2,1 dist=1 held=0 speaker=1 net=host host=he_is_the_cat " \
        "me=he_is_the_cat members=he_is_the_cat,Bob ai=he_is_the_cat"


async def main():
    async with websockets.connect(URL, max_size=None) as ws:
        async def send(m):
            await ws.send(json.dumps(m))

        async def drain(secs):
            end = time.time() + secs
            got = []
            while time.time() < end:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=max(0.05, end - time.time()))
                except asyncio.TimeoutError:
                    break
                if isinstance(raw, bytes):
                    continue
                m = json.loads(raw)
                if m.get("t") == "reply" and (m.get("say") or m.get("orders")):
                    got.append(m)
                    print(f"   REPLY ({m.get('how')}): {m.get('say')!r} orders={m.get('orders')}")
                elif m.get("t") == "log" and any(k in m["text"] for k in ("report", "learned", "chat model", "thinking", "dropped")):
                    print("   log:", m["text"])
            return got

        async def state():
            await send({"t": "dll", "line": STATE})

        await send({"t": "hello", "v": 1, "name": "he_is_the_cat", "tz": 60, "build": "test"})
        await state()
        await drain(1)
        tests = [
            ("you", "what is your favourite colour"),
            ("you", "what is your favourite colour"),
            ("you", "do you think pineapple belongs on pizza"),
            ("you", "can you fly over to me please"),
            ("Bob", "edith what do you think about the new combat map"),
        ]
        for who, text in tests:
            await state()
            print(f"\n{who}: {text}")
            t0 = time.time()
            msg = {"t": "text", "text": text}
            if who != "you":
                msg["who"] = who
            await send(msg)
            got = await drain(20)
            print(f"   ({time.time() - t0:.1f} s, {len(got)} replies)")
        for line in ("heard Bob you are trash, kys", "heard Bob this game is so bugged, I fell through the floor again"):
            await state()
            print(f"\noverheard: {line}")
            await send({"t": "dll", "line": line})
            await drain(3)
        await state()
        print("\nspawned (owner)")
        await send({"t": "dll", "line": "spawned"})
        await drain(4)


asyncio.run(main())
for path in ("stats", "reports", "learned"):
    if path != "stats" and not TOKEN:
        continue
    with urllib.request.urlopen(f"{HTTP}/edith/brain/{path}?token={TOKEN}") as r:
        print(f"\n/{path}:", r.read().decode()[:900])

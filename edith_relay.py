"""Edith relay -- one shared satellite (Edith, EchoEnhancements' Satellite.dll) per Echo match.

Every player running Satellite.dll keeps one WebSocket open here:

    wss://<host>/edith/ws

1. On connect, and whenever their match changes, the plugin sends a text HELLO:

       {"v": 1, "t": "hello", "name": "<own display name>", "roster": ["<name>", ...],
        "ai": true|false, "build": "<plugin build>"}

   `roster` is every other player in their current Echo match; `ai` says their voice companion runs.

2. The relay groups players who BOTH list each other (the same match) and picks one HOST per group:
   players whose companion runs first, then whoever has been in the group longest. Each member gets

       {"t": "room", "host": "<name>", "members": ["<name>", ...], "ai": ["<name>", ...]}

   whenever their group or its host changes. `ai` lists the members whose companion runs: the host
   hears them through their own companion, never through voice chat (no double answers). The host runs her brain; everyone else follows it.

3. Messages are forwarded inside a group only, stamped with the sender's name:
   - binary b"EDS1"... (her state, 20 Hz) from any member -> everyone else in the group
     (the host, or the player holding her); b"EDP1"... (voice audio) from the host only.
     What arrives is b"EDR1" + len(u8) + name(utf-8) + the original message.
   - text {"t": "voice"|"snow"|"face", ...} from the host -> everyone else
   - text {"t": "ctl", "to": "<name>", ...} from the host -> that player only
   - text {"t": "touch"|"heard", ...} from anyone -> the host only
   - text {"t": "ball", "k": <0-15>, "pos": [x,y,z], "vel": [x,y,z], "held": 0|1} from any member
     (whoever holds one of her balls: 15/s while held, once more on release) -> everyone else.
     Its own limit (MAX_BALLS_PER_SEC), apart from the other text messages' MAX_EVENTS_PER_SEC.
   Text arrives as the same JSON with "from": "<sender>" added.

Nothing is stored. A name can only be held by one connection; the newest wins.
Per-connection limits keep a misbehaving client from flooding anyone.
"""
from __future__ import annotations

import asyncio
import json
import time
from typing import Dict, List, Optional, Set

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

router = APIRouter()

MAX_CLIENTS = 1000
MAX_NAME = 48
MAX_ROSTER = 32
MAX_TEXT = 4096
MAX_BINARY = 2048
MAX_STATE_PER_SEC = 40      # the plugin sends 20
MAX_PCM_PER_SEC = 80        # 20 ms chunks: 50/s while she talks
MAX_EVENTS_PER_SEC = 20
MAX_BALLS_PER_SEC = 30      # the plugin sends 15 while a ball is held
MAX_HELLOS_PER_MIN = 60
SEND_QUEUE = 64             # messages buffered per receiver before old ones are dropped
HOST_TO_GROUP = {"voice", "snow", "face"}
TO_HOST = {"touch", "heard"}
ANY_TO_GROUP = {"ball"}


class Client:
    def __init__(self, ws: WebSocket):
        self.ws = ws
        self.name: Optional[str] = None
        self.roster: Set[str] = set()
        self.ai = False
        self.build = ""
        self.since = time.monotonic()        # when it joined (host order)
        self.room: Optional[str] = None      # the last room message sent (only resent on change)
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=SEND_QUEUE)
        self.windows: Dict[str, tuple] = {}
        self.closed = False

    def rate_ok(self, which: str, limit: int, period: float) -> bool:
        start, n = self.windows.get(which, (0.0, 0))
        now = time.monotonic()
        if now - start >= period:
            self.windows[which] = (now, 1)
            return True
        if n >= limit:
            return False
        self.windows[which] = (start, n + 1)
        return True

    def offer(self, data) -> None:
        """Queue a message (bytes or str), dropping the oldest if the receiver is falling behind."""
        if self.closed:
            return
        if self.queue.full():
            try:
                self.queue.get_nowait()
            except asyncio.QueueEmpty:
                pass
        try:
            self.queue.put_nowait(data)
        except asyncio.QueueFull:
            pass


_by_name: Dict[str, Client] = {}
_count = 0


def _clean_name(value) -> Optional[str]:
    if not isinstance(value, str):
        return None
    value = value.strip()
    if not value or len(value.encode("utf-8")) > MAX_NAME:
        return None
    return value


def _group(client: Client) -> List[Client]:
    """The client and every connected player it and they both list: its match."""
    out = [client]
    for name in client.roster:
        peer = _by_name.get(name)
        if peer is not None and peer is not client and not peer.closed and client.name in peer.roster:
            out.append(peer)
    return out


def _host(group: List[Client]) -> Client:
    return min(group, key=lambda c: (not c.ai, c.since, c.name))


def _send_rooms(names: Set[str]) -> None:
    """Re-send the room to these players if theirs changed."""
    for name in names:
        c = _by_name.get(name)
        if c is None or c.closed:
            continue
        group = _group(c)
        room = json.dumps({"t": "room", "host": _host(group).name,
                           "members": sorted(m.name for m in group),
                           "ai": sorted(m.name for m in group if m.ai)}, separators=(",", ":"))
        if room != c.room:
            c.room = room
            c.offer(room)


def _affected(client: Client) -> Set[str]:
    """Players whose room may change when this client joins, leaves or changes roster."""
    names = set(client.roster)
    if client.name:
        names.add(client.name)
        names |= {n for n, c in _by_name.items() if client.name in c.roster}
    return names


async def _sender(client: Client) -> None:
    try:
        while not client.closed:
            data = await client.queue.get()
            if isinstance(data, bytes):
                await client.ws.send_bytes(data)
            else:
                await client.ws.send_text(data)
    except Exception:
        client.closed = True


def _release(client: Client) -> None:
    if client.name and _by_name.get(client.name) is client:
        del _by_name[client.name]


def _hello(client: Client, msg: dict) -> None:
    name = _clean_name(msg.get("name"))
    roster = msg.get("roster") or []
    if not name or not isinstance(roster, list):
        return
    before = _affected(client)
    if client.name != name:
        _release(client)
        old = _by_name.get(name)
        if old is not None and old is not client:
            old.closed = True                  # newest connection for a name wins
            _release(old)
        client.name = name
        client.since = time.monotonic()
        _by_name[name] = client
    client.roster = {n for n in (_clean_name(r) for r in roster[:MAX_ROSTER]) if n and n != name}
    client.ai = bool(msg.get("ai"))
    client.build = str(msg.get("build") or "")[:32]
    client.room = None                         # always answer a hello
    _send_rooms(before | _affected(client))


def _route_text(client: Client, msg: dict) -> None:
    t = msg.get("t")
    group = _group(client)
    host = _host(group)
    msg["from"] = client.name
    out = json.dumps(msg, separators=(",", ":"))
    if t in ANY_TO_GROUP or (t in HOST_TO_GROUP and client is host):
        for peer in group:
            if peer is not client:
                peer.offer(out)
    elif t == "ctl" and client is host:
        target = _by_name.get(_clean_name(msg.get("to")) or "")
        if target is not None and target in group:
            target.offer(out)
    elif t in TO_HOST and client is not host:
        host.offer(out)


def _route_binary(client: Client, data: bytes) -> None:
    kind = data[:4]
    group = _group(client)
    if kind == b"EDS1":
        if not client.rate_ok("state", MAX_STATE_PER_SEC, 1.0):
            return
    elif kind == b"EDP1":
        if _host(group) is not client or not client.rate_ok("pcm", MAX_PCM_PER_SEC, 1.0):
            return
    else:
        return
    name_b = client.name.encode("utf-8")
    out = b"EDR1" + bytes([len(name_b)]) + name_b + data
    for peer in group:
        if peer is not client:
            peer.offer(out)


@router.websocket("/edith/ws")
async def edith_ws(ws: WebSocket):
    global _count
    if _count >= MAX_CLIENTS:
        await ws.close(code=1013)
        return
    await ws.accept()
    _count += 1
    client = Client(ws)
    sender = asyncio.create_task(_sender(client))
    try:
        while not client.closed:
            msg = await ws.receive()
            if msg.get("type") == "websocket.disconnect":
                break
            text = msg.get("text")
            if text is not None:
                if len(text) > MAX_TEXT:
                    continue
                try:
                    data = json.loads(text)
                except ValueError:
                    continue
                if not isinstance(data, dict):
                    continue
                if data.get("t") == "hello":
                    if client.rate_ok("hello", MAX_HELLOS_PER_MIN, 60.0):
                        _hello(client, data)
                elif data.get("t") in ANY_TO_GROUP:
                    if client.name and client.rate_ok("ball", MAX_BALLS_PER_SEC, 1.0):
                        _route_text(client, data)
                elif client.name and client.rate_ok("events", MAX_EVENTS_PER_SEC, 1.0):
                    _route_text(client, data)
                continue
            data = msg.get("bytes")
            if data is not None and client.name and 4 <= len(data) <= MAX_BINARY:
                _route_binary(client, data)
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        client.closed = True
        affected = _affected(client)
        _release(client)
        _send_rooms(affected)
        sender.cancel()
        _count -= 1


@router.get("/edith/stats")
async def edith_stats():
    """How many plugins are connected and how many hosts -- counts only, no names."""
    hosts = {_host(_group(c)).name for c in _by_name.values() if not c.closed}
    return {"connected": _count, "named": len(_by_name), "hosts": len(hosts)}

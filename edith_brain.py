"""Edith's brain -- her hearing, rules, memory and choice of words, server-side (EchoEnhancements).

Every player's companion (EchoEnhancements ai/edith_ears.py) keeps one WebSocket open here:

    wss://<host>/edith/brain

The companion is thin: mic + VAD, the game's events, and playing her lines. Everything else lives
here, so she can be changed live without touching anyone's PC (EchoEnhancements docs/brainplan.md):
Whisper (faster-whisper base.en, int8, CPU), the wake word and follow-up window, her rules (orders,
refusals and stubbornness, points per player, the favourite, teaching, memory), and her library
(edith_library/*.json, reloaded whenever a file changes; editable over HTTP with ADMIN_TOKEN).

Companion -> brain (text JSON unless noted):
    {"t":"hello","v":1,"name":"<own name or ''>","tz":<minutes east of UTC>,"build":"..."}
    {"t":"dll","line":"<a line from Satellite.dll's event port>"}    state / event / mute / heard
    binary b"EDA1" + u8 len + name + int16 mono 16 kHz              one utterance ('' name = own mic)
    {"t":"text","text":"edith come here"}                           typed (tests): no wake word needed
    {"t":"speaking","on":true|false}                                her line started / finished playing
    {"t":"render","id":<n>,"text":"..."}                            a line the voice pack lacks
Brain -> companion:
    {"t":"hi","name":"Edith","voice":"hera_f5","fallback":"af_heart","speed":1.1,"stt":bool,"tts":bool}
    {"t":"reply","say":"...","key":"<sha1 20>","alt":"<sha1 20>","parts":[[text,key,alt],...],
     "face":[mood,seconds]|null,"orders":["come Bob","favour Ann",...],"how":"...","stream":true}
        "stream": true (only present when true): the bank copy of this line is broken (revoice.json,
        or its stream_all). The game must NOT play key/alt/parts from its voice banks; it asks for
        the whole "say" with {"t":"render"} as for a line the pack lacks, and plays the EDV1 stream
        (the server's Kokoro af_heart render, cached by text, so usually instant). Older games
        ignore the field and play the bank copy.
    binary b"EDV1" + u32 id + int16 mono 24 kHz                     streamed render (Kokoro af_heart)
    {"t":"rendered","id":<n>,"ok":bool}
    {"t":"log","text":"..."}                                        what she heard / did, for its console

Her orders (Satellite.dll / EDITH.dll) she sends of her own accord (her initiative, user 2026-10-19):
    approach <name>       fly up to that player          } only when the state line reports
    follow crowd          follow the middle of everyone  } players=<name>:x,y,z;<name>:x,y,z;...
    roam                  free roam (her 2018 waypoints) }
    come <name> + lookat 8 <name>                        the fallback without players= (older DLLs)

Memory and points are kept per player name: Postgres (table edith_players) when DATABASE_URL is
set, else JSON files in $DATA_DIR/edith_brain/players.

What her library can't answer goes to her small chat model (edith_mind.py, Qwen2.5-0.5B on the CPU),
off the event loop; its replies are kept as answers she learned herself (record "#edith", "auto"), so
the same question next time is instant. She reports toxic players and bug reports she overhears
(edith_mind.py: Discord webhook, and her owner hears them when they next spawn in her lobby).
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import math
import os
import random
import re
import shutil
import struct
import threading
import time
from difflib import SequenceMatcher
from pathlib import Path
from typing import Dict, List, Optional

from fastapi import APIRouter, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import PlainTextResponse

from edith_barrier import Barrier
from edith_mind import CHAT, OWNER, detect, opener_prompt, post_webhook, system_prompt, tell_line, tidy

router = APIRouter()

HERE = Path(__file__).resolve().parent
DATA = Path(os.environ.get("DATA_DIR", ".")) / "edith_brain"
SEED = HERE / "edith_library"                                     # the library as deployed
LIB_DIR = Path(os.environ.get("EDITH_LIBRARY") or (DATA / "library"))   # the live one (editable)
LIB_FILES = ("config", "lines", "knowledge", "smalltalk", "reactions", "chat", "sentiment", "barrier", "revoice")
DATABASE_URL = os.environ.get("DATABASE_URL")
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN")

MAX_SESSIONS = 300
MAX_AUDIO = 16000 * 2 * 13              # 13 s of int16 16 kHz
MAX_AUDIO_PER_MIN = 30
MAX_TEXT = 8192
MAX_RENDERS_PER_MIN = 30


def log(*a):
    print(time.strftime("[%H:%M:%S] edith"), *a, flush=True)


# ---- the library (was ai/library.py) -------------------------------------------------------------

DEFAULT_CFG = {
    "name": "Edith",
    "wake_words": ["edith", "edit", "edyth", "edeth", "eadith", "eddith", "editha", "edie", "eadie", "satellite"],
    "follow_up_seconds": 8, "react": True, "mean": True, "push_after": 2, "shutdown_after": 5,
    "ignore_below": 5.0, "cancel_after": 3, "mean_reset_s": 300, "ask_sorry_every_s": 45, "order_hold_s": 60,
    "voice": "hera_f5", "fallback_voice": "af_heart", "voice_speed": 1.1,
    "unknown": "Sorry i dont quite know that one yet",
    "initiative": {"on": True, "idle_s": 75, "every_s": 150, "listen_s": 20, "after_order_s": 120,
                   "roam_chance": 0.25, "sorry_every_s": 90, "weights": {"liked": 0.4, "quiet": 0.35, "crowd": 0.25}},
}

STOP = set("""a an the is are was were be been am i you he she it we they me my your our their this that
these those to of in on at for with and or but do does did can could would should will shall what whats
who whos how hows when where why which there here please tell about know any some so just really very
cubie edith hey hi yo okay ok like give get got have has had its it's im i'm youre you're""".split())


def norm(text: str) -> str:
    t = text.lower().replace("’", "'")
    t = re.sub(r"[^a-z0-9' ]+", " ", t)
    t = re.sub(r"\b(two|ii)\b", "2", t)                     # Lone Echo II = two = 2
    return re.sub(r"\s+", " ", t).strip()


def words(text: str) -> list:
    return [w.strip("'") for w in norm(text).split() if w.strip("'")]


def stem(w: str) -> str:
    for suf in ("ing", "ed", "es", "s"):
        if len(w) > 4 and w.endswith(suf):
            return w[: -len(suf)]
    return w


def terms(text: str) -> list:
    return [stem(w) for w in words(text) if w not in STOP]


def _alts(items: list) -> str:
    return "|".join(sorted(items, key=len, reverse=True))


class Library:
    """Her library: small talk, Echo VR / Lone Echo knowledge, the chat bank, reactions, her fixed
    lines, the kind / mean words, and her settings. Read-only once built (a new one replaces it on
    a file change); what changes per Edith (the last line picked, the reaction cooldown) is hers."""

    def __init__(self, files: Dict[str, dict]):
        self.cfg = dict(DEFAULT_CFG)
        self.cfg.update({k: v for k, v in files.get("config", {}).items() if not k.startswith("_")})
        self.lines = {k: v for k, v in files.get("lines", {}).items() if isinstance(v, list)}
        k, s, r = files["knowledge"], files["smalltalk"], files["reactions"]
        self.entries = k["entries"]
        self.intents = [(i["id"], [re.compile(p) for p in i["patterns"]], i["replies"]) for i in s["intents"]]
        self.reactions = r["events"]
        self.dizzy = {int(x): v for x, v in r.get("dizzy", {}).items()}
        self.moods = {x: v for x, v in r.items() if isinstance(v, list)}
        self.cooldown = float(r.get("cooldown_s", 3))
        self.index = []
        df: Dict[str, int] = {}
        for e in self.entries:
            tf: Dict[str, float] = {}
            for key in e.get("keys", []):
                for t in terms(key):
                    tf[t] = tf.get(t, 0) + 3.0
            for q in e.get("q", []):
                for t in terms(q):
                    tf[t] = tf.get(t, 0) + 1.5
            for a in e.get("a", []):
                for t in set(terms(a)):
                    tf[t] = tf.get(t, 0) + 0.3
            phrases = [norm(x) for x in e.get("keys", []) + e.get("q", [])]
            self.index.append((e, tf, phrases, [norm(x) for x in e.get("q", [])]))
            for t in tf:
                df[t] = df.get(t, 0) + 1
        n = max(1, len(self.entries))
        self.idf = {t: math.log(1 + n / c) for t, c in df.items()}
        self.chats = [(e, self.examples(e["q"])) for e in files.get("chat", {"entries": []})["entries"]]
        sent = files["sentiment"]
        self.nice_words, self.mean_words = _alts(sent["nice_words"]), _alts(sent["mean_words"])
        self.nice_phrases = re.compile(r"\b(" + _alts(sent["nice_phrases"]) + r")\b")
        self.mean_phrases = re.compile(r"\b(" + _alts(sent["mean_phrases"]) + r")\b")
        self.word_res = []
        for ws, sign in ((self.mean_words, -1), (self.nice_words, 1)):
            self.word_res.append((re.compile(
                rf"\b(?:(not|never|isn'?t|aren'?t|ain'?t|no)\s+(?:\w+\s+)?)?"
                rf"(?:(?:so|really|very|such|super|fucking|freaking|totally|absolutely|literally|kinda|actually) )*"
                rf"(?:such a |a |an |the )?({ws})\b"), sign))
        self.barrier = Barrier(files.get("barrier", {}))
        rv = files.get("revoice", {})
        self.stream_all = bool(rv.get("stream_all", False))
        self.revoice = [e for e in rv.get("lines", []) if isinstance(e, dict) and (e.get("text") or e.get("key"))]
        c = self.cfg
        self.revoice_keys = set()
        for e in self.revoice:                     # its keys as marked, and as the voice settings make them now
            self.revoice_keys |= {e.get("key"), e.get("alt")} - {None, ""}
            if e.get("text"):
                self.revoice_keys |= {voice_key(c["voice"], c["voice_speed"], e["text"]),
                                      voice_key(c["fallback_voice"], c["voice_speed"], e["text"])}

    def must_stream(self, keys) -> bool:
        """A reply with one of these keys streams the server's voice (revoice.json; user, 2026-10-19)."""
        return self.stream_all or any(k in self.revoice_keys for k in keys)

    def ini(self) -> dict:
        """Her initiative settings (config.json "initiative", over the defaults)."""
        out = dict(DEFAULT_CFG["initiative"])
        got = self.cfg.get("initiative")
        if isinstance(got, dict):
            out.update(got)
        return out

    def line(self, key: str, default: Optional[list] = None) -> list:
        return self.lines.get(key) or default or [""]

    # -- picking lines --
    @staticmethod
    def pick(picks: dict, key: str, lines: list) -> str:
        """A random line, never the same as the last one for this key (per Edith)."""
        if len(lines) > 1:
            lines = [l for l in lines if l != picks.get(key)] or lines
        line = random.choice(lines)
        picks[key] = line
        return line

    def all_lines(self, knowledge: bool = True) -> list:
        """Every stock line (for the voice pack's list)."""
        out = []
        for _, _, replies in self.intents:
            out += replies
        for ev in self.reactions.values():
            for lines in ev.values():
                out += lines
        for lines in list(self.dizzy.values()) + list(self.moods.values()) + list(self.lines.values()):
            out += lines
        for e, _ in self.chats:
            out += e["a"]
        if knowledge:
            for e in self.entries:
                out += e.get("a", [])
        return out

    # -- the chat bank --
    @staticmethod
    def _chat_norm(text: str) -> str:
        t = norm(text)
        t = re.sub(r"^((hey|oi|yo|um|uh|so|okay|ok|well|and|but)\s+)+", "", t)
        t = re.sub(r"^((cubie|edith)\s+)+|\s+(cubie|edith)$", "", t)
        t = re.sub(r"\s+(please|buddy|mate|bro|dude)$", "", t)
        t = re.sub(r"\b(really|so|very|super|just|actually|kinda|kind of|literally|honestly|today|right now|"
                   r"at the moment|tonight|like)\b", " ", t)
        return re.sub(r"\s+", " ", t).strip()

    def examples(self, phrasings: list) -> list:
        qs = [self._chat_norm(q) for q in phrasings]
        return [(q, set(words(q)), set(terms(q))) for q in qs if q]

    def chat(self, picks: dict, text: str, threshold: float = 0.67, margin: float = 0.03, pool=None):
        """The chat bank's best entry -> (score, id, reply, order or ""), or None (see ai/library.py)."""
        t = self._chat_norm(text)
        if not t or len(t.split()) > 14:
            return None
        tw, tt = set(words(t)), set(terms(t))
        scored = []
        for e, examples in (self.chats if pool is None else pool):
            best = 0.0
            for q, qw, qt in examples:
                r = SequenceMatcher(None, t, q).ratio()
                j = len(tw & qw) / max(1, len(tw | qw))
                best = max(best, 0.55 * r + 0.45 * j + (0.1 if qt and qt <= tt else 0.0))
            scored.append((best, e))
        if not scored:
            return None
        scored.sort(key=lambda x: -x[0])
        best, e = scored[0]
        second = scored[1][0] if len(scored) > 1 else 0.0
        if best < threshold or (best - second < margin and best < 0.85):
            return None
        return best, e["id"], self.pick(picks, "chat/" + e["id"], e["a"]), e.get("do", "")

    # -- small talk --
    def smalltalk(self, picks: dict, text: str):
        t = norm(text)
        t = re.sub(r"^(cubie|edith|hey cubie|hey edith|oi|um|uh|so)\s+", "", t).strip()
        t = re.sub(r"\s+(cubie|edith|please|buddy)$", "", t).strip()
        if not t or len(t.split()) > 9:
            return None
        for iid, pats, replies in self.intents:
            if any(p.search(t) for p in pats):
                return iid, self.pick(picks, iid, replies)
        return None

    # -- knowledge --
    def score(self, text: str):
        q = terms(text)
        nq = norm(text)
        if not q:
            return [], None
        out, exact = [], None
        for e, tf, phrases, qs in self.index:
            s = sum(tf.get(t, 0) * self.idf.get(t, 0) for t in set(q))
            for ph in phrases:
                if ph and len(ph) > 3 and ph in nq:
                    s += 6.0 + 2.0 * len(ph.split())
            if nq in qs:
                exact = e["id"]
                s += 20.0
            if s > 0:
                out.append((s / (1.0 + 0.15 * len(q)), e))
        out.sort(key=lambda x: -x[0])
        return out, exact

    def answer(self, picks: dict, text: str, threshold: float = 9.0):
        ranked, exact = self.score(text)
        if not ranked:
            return None
        best, e = ranked[0]
        second = ranked[1][0] if len(ranked) > 1 else 0.0
        if best < threshold or (best < second * 1.15 and exact != e["id"]):
            return None
        return best, e["id"], self.pick(picks, e["id"], e["a"])

    # -- reactions --
    def reaction(self, ed: "Edith", event: str, count_today: int):
        ev = self.reactions.get(event)
        if not ev:
            return None
        now = time.time()
        if now - ed.last_reaction < self.cooldown:
            return None
        ed.last_reaction = now
        tier = "first" if count_today <= 1 else "again" if count_today <= 5 else "lots"
        return self.pick(ed.picks, f"{event}/{tier}", ev.get(tier) or ev["first"])

    def dizzy_line(self, picks: dict, seconds: int):
        if not self.dizzy:
            return None
        tier = max(t for t in self.dizzy if t <= seconds) if any(t <= seconds for t in self.dizzy) else min(self.dizzy)
        return self.pick(picks, f"dizzy/{tier}", self.dizzy[tier])

    def mood(self, picks: dict, key: str):
        lines = self.moods.get(key)
        return self.pick(picks, key, lines) if lines else None

    # -- kind and mean words --
    def sentiment(self, text: str, addressed: bool) -> int:
        """+1 kind, -1 mean, 0 neither, about her (see ai/satellite_ai.py sentiment())."""
        t = " " + re.sub(r"[^a-z' ]", " ", text.lower()) + " "
        about = re.search(ABOUT_HER, t) or (addressed and (re.search(TO_HER, t) or len(t.split()) <= 3))
        if addressed or about:
            if self.mean_phrases.search(t):
                return -1
            if self.nice_phrases.search(t):
                return 1
        if not about or len(t.split()) > 12:                       # (a long explanation to a friend isn't name-calling)
            return 0
        for rx, sign in self.word_res:
            m = rx.search(t)
            if m:
                return -sign if m.group(1) else sign
        return 0


TO_HER = r"\b(you|you'?re|youre|your|u|ur|ya)\b"
ABOUT_HER = r"\b(she|she'?s|shes|her|edith|edie|cubie|(this|that|the|dumb|stupid|cute) (thing|robot|bot|drone|satellite|cube|cubesat|sat))\b"


def _sha(b: bytes) -> str:
    return hashlib.sha1(b).hexdigest()


def seed_library():
    """Copy the deployed library into the live folder: a missing file, or one the deploy changed
    that nobody edited live since (a live edit wins; it's logged)."""
    LIB_DIR.mkdir(parents=True, exist_ok=True)
    if LIB_DIR.resolve() == SEED.resolve():
        return
    marks_f = LIB_DIR / ".seeded.json"
    try:
        marks = json.loads(marks_f.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        marks = {}
    for name in LIB_FILES:
        src, dst = SEED / f"{name}.json", LIB_DIR / f"{name}.json"
        if not src.exists():
            continue
        s = _sha(src.read_bytes())
        if not dst.exists():
            shutil.copyfile(src, dst)
        elif marks.get(name) != s:
            live = _sha(dst.read_bytes())
            if live == marks.get(name) or live == s or name not in marks:
                shutil.copyfile(src, dst)
            else:
                log(f"library: {name}.json was edited live; the deploy's new copy is not applied")
                continue
        marks[name] = s
    marks_f.write_text(json.dumps(marks), encoding="utf-8")


class LibraryHolder:
    """The live library, rebuilt when a file in LIB_DIR changes (checked at most every 2 s). A file
    that doesn't parse keeps the previous library (and is logged)."""

    def __init__(self):
        self.lib: Optional[Library] = None
        self.stamp = None
        self.checked = 0.0
        self.error = ""
        self.lock = threading.Lock()

    def _stamp(self):
        return tuple((n, (LIB_DIR / f"{n}.json").stat().st_mtime_ns if (LIB_DIR / f"{n}.json").exists() else 0)
                     for n in LIB_FILES)

    @staticmethod
    def read(folder: Path, override: Optional[Dict[str, dict]] = None) -> Dict[str, dict]:
        files = {}
        for n in LIB_FILES:
            if override and n in override:
                files[n] = override[n]
            elif (folder / f"{n}.json").exists():
                files[n] = json.loads((folder / f"{n}.json").read_text(encoding="utf-8"))
        return files

    def get(self) -> Library:
        now = time.time()
        if self.lib is not None and now - self.checked < 2.0:
            return self.lib
        with self.lock:
            self.checked = now
            try:
                st = self._stamp()
                if st != self.stamp or self.lib is None:
                    lib = Library(self.read(LIB_DIR))
                    if self.lib is not None:
                        log(f"library reloaded: {len(lib.entries)} knowledge, {len(lib.intents)} small talk, "
                            f"{len(lib.chats)} chat entries")
                    self.lib, self.stamp, self.error = lib, st, ""
            except Exception as e:  # noqa: BLE001 -- a bad edit: keep her old library
                err = f"{type(e).__name__}: {e}"[:300]
                if err != self.error:
                    log("library: not reloaded,", err)
                self.error = err
                if self.lib is None:
                    self.lib = Library(self.read(SEED))
        return self.lib


LIB = LibraryHolder()


# ---- what she may say ----------------------------------------------------------------------------

def say_clean(text: str) -> str:
    """No emoji or other symbols, no stage directions, never the word "coming" (user, 2026-10-17)."""
    t = re.sub(r"\*[^*]*\*|\([^)]*\)|\[[^\]]*\]", " ", text)
    t = t.replace("’", "'").replace("‘", "'").replace("“", '"').replace("”", '"')
    t = t.replace("_", " ").replace("—", ", ").replace("–", ", ").replace("…", "...")
    t = "".join(ch for ch in t if ch.isascii() and (ch.isalnum() or ch in " .,!?'\"-:;"))
    t = re.sub(r"\b[Cc]oming\b", lambda m: "on my way" if m.group(0)[0] == "c" else "On my way", t)
    t = re.sub(r"\s+([.,!?])", r"\1", re.sub(r"\s+", " ", t)).strip(" ,;:-")
    return t


def drop_name(line: str) -> str:
    """She never says anyone's name (user, 2026-10-18): "{name}" goes, with its comma."""
    t = re.sub(r",\s*\{name\}(?=[\s.!?,]|$)", "", line)
    t = re.sub(r"(^|(?<=[.!?]\s))\{name\},?\s*(\w)", lambda m: m.group(1) + m.group(2).upper(), t)
    t = re.sub(r"\s*\{name\}", "", t)
    return re.sub(r"\s+([.!?,])", r"\1", t).strip()


def voice_key(voice: str, speed, text: str) -> str:
    """The voice cache / pack file name: the same as ai/satellite_ai.py Voice._file."""
    return hashlib.sha1(f"{voice}|{speed}|{text}".encode()).hexdigest()[:20]


def sentences(text: str) -> list:
    return [p for p in re.split(r"(?<=[.!?])\s+", text) if p]


def you_form(text: str) -> str:
    swaps = {"i": "you", "i'm": "you're", "im": "you're", "i've": "you've", "i'll": "you'll", "i'd": "you'd",
             "me": "you", "my": "your", "mine": "yours", "myself": "yourself", "am": "are", "was": "were"}
    return " ".join(swaps.get(w.lower(), w) for w in text.split())


# ---- per-player records: memory, profile, points --------------------------------------------------

def blank_record() -> dict:
    return {"facts": [], "events": [], "turns": [], "places": {}, "learned": [],
            "profile": {"nickname": "", "likes": [], "dislikes": [], "favorites": {}, "notes": []},
            "points": {"nice": 0, "mean": 0.0, "mean_at": time.time(), "streak": 0}}


class Store:
    """Player name -> record (her memory of them). Postgres when DATABASE_URL is set, else one JSON
    file per player. Changes are written every 2 s (flush), off the event loop."""

    def __init__(self):
        self.recs: Dict[str, dict] = {}
        self.dirty: set = set()
        self.db = False
        self.folder = DATA / "players"
        if DATABASE_URL:
            try:
                import psycopg2
                with psycopg2.connect(DATABASE_URL) as conn:
                    with conn.cursor() as cur:
                        cur.execute("""CREATE TABLE IF NOT EXISTS edith_players (
                                         name TEXT PRIMARY KEY, data JSONB NOT NULL,
                                         updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())""")
                    conn.commit()
                self.db = True
            except Exception as e:  # noqa: BLE001
                log("store: no database, files instead:", e)
        if not self.db:
            self.folder.mkdir(parents=True, exist_ok=True)

    def _file(self, name: str) -> Path:
        return self.folder / f"{re.sub(r'[^A-Za-z0-9_-]', '_', name)[:40]}-{_sha(name.encode())[:8]}.json"

    def _load(self, name: str) -> Optional[dict]:
        try:
            if self.db:
                import psycopg2
                with psycopg2.connect(DATABASE_URL) as conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT data FROM edith_players WHERE name = %s", (name,))
                        row = cur.fetchone()
                return row[0] if row else None
            f = self._file(name)
            return json.loads(f.read_text(encoding="utf-8")) if f.exists() else None
        except Exception as e:  # noqa: BLE001
            log(f"store: can't read {name}:", e)
            return None

    def get(self, name: str) -> dict:
        rec = self.recs.get(name)
        if rec is None:
            rec = blank_record()
            got = self._load(name)
            if isinstance(got, dict):
                for k, v in got.items():
                    if isinstance(v, dict) and isinstance(rec.get(k), dict):
                        rec[k].update(v)
                    else:
                        rec[k] = v
            self.recs[name] = rec
        return rec

    def mark(self, name: str):
        if name in self.recs:
            self.dirty.add(name)

    def take(self) -> list:
        """The changed records, serialized now (on the loop), to be written by write()."""
        out = [(n, json.dumps(self.recs[n])) for n in self.dirty if n in self.recs]
        self.dirty.clear()
        return out

    def write(self, items: list):
        if not items:
            return
        try:
            if self.db:
                import psycopg2
                with psycopg2.connect(DATABASE_URL) as conn:
                    with conn.cursor() as cur:
                        for n, js in items:
                            cur.execute("""INSERT INTO edith_players (name, data, updated_at) VALUES (%s, %s, NOW())
                                           ON CONFLICT (name) DO UPDATE SET data = EXCLUDED.data, updated_at = NOW()""",
                                        (n, js))
                    conn.commit()
            else:
                for n, js in items:
                    f = self._file(n)
                    tmp = f.with_suffix(".tmp")
                    tmp.write_text(js, encoding="utf-8")
                    tmp.replace(f)
        except Exception as e:  # noqa: BLE001
            log("store: write failed:", e)

    def forget(self, name: str):
        self.recs.pop(name, None)
        self.dirty.discard(name)
        try:
            if self.db:
                import psycopg2
                with psycopg2.connect(DATABASE_URL) as conn:
                    with conn.cursor() as cur:
                        cur.execute("DELETE FROM edith_players WHERE name = %s", (name,))
                    conn.commit()
            elif self._file(name).exists():
                self._file(name).unlink()
        except Exception as e:  # noqa: BLE001
            log("store: forget failed:", e)


STORE: Optional[Store] = None


def store() -> Store:
    global STORE
    if STORE is None:
        STORE = Store()
    return STORE


#: her own record in the store, beside the players' (never a player's name: Echo names have no "#"):
#: "auto" what she learned by herself (her chat model's answers), "reports" what she reported
SELF = "#edith"
AUTO_MAX, REPORTS_MAX = 3000, 300
_auto = {"ver": 0, "built": None, "lib": None, "pool": []}
_report_seen: Dict[tuple, float] = {}                          # (who, kind) -> when: one rant, one report


def self_rec() -> dict:
    rec = store().get(SELF)
    rec.setdefault("auto", [])
    rec.setdefault("reports", [])
    return rec


def auto_changed():
    _auto["ver"] += 1
    store().mark(SELF)


def auto_pool(lib: "Library") -> list:
    """Her self-learned answers as a chat pool (rebuilt when they or the library change)."""
    if _auto["built"] != _auto["ver"] or _auto["lib"] is not lib:
        _auto["built"], _auto["lib"] = _auto["ver"], lib
        _auto["pool"] = [(e, lib.examples(e["q"])) for e in self_rec()["auto"]]
    return _auto["pool"]


def auto_find(eid: str) -> Optional[dict]:
    return next((e for e in self_rec()["auto"] if e["id"] == eid), None)


class Person:
    """One player as she sees them: points (user, 2026-10-18; docs/networkplan.md "Who she listens
    to"). Score = nice - mean; nice points are kept until cancel_after bad things in a row wipe
    them. Mean points reset fully after mean_reset_s (5 min) without a new bad act, or at once on a
    real apology (user, 2026-10-19). Points are saved with the player's record; the rest (follow-up
    window, a stubborn ask, ...) lives while the server runs."""

    def __init__(self, name: str):
        self.name = name
        self.saved = name != "you"                                     # "you" = a name not known yet
        self.pts = store().get(name)["points"] if self.saved else blank_record()["points"]
        self.bad_acts = 0
        self.last_spoke = 0.0
        self.awake_until = 0.0
        self.asked, self.asked_at = "", 0.0
        self.last_snow = 0.0
        self.asked_sorry_at = 0.0

    nice =property(lambda s: s.pts["nice"], lambda s, v: s._set("nice", v))
    mean = property(lambda s: s.pts["mean"], lambda s, v: s._set("mean", v))
    streak = property(lambda s: s.pts["streak"], lambda s, v: s._set("streak", v))

    def _set(self, k, v):
        self.pts[k] = v
        if self.saved:
            store().mark(self.name)

    def _fade(self):
        """Mean points go all at once, mean_reset_s after the last bad act (mean_at)."""
        if self.mean <= 0:
            return
        reset = float(LIB.get().cfg.get("mean_reset_s", 300) or 300)
        if time.time() - self.pts.get("mean_at", 0) >= reset:
            self.mean, self.streak, self.bad_acts = 0.0, 0, 0

    def forgive(self):
        """A real apology: mean points (and the bad streak) gone."""
        self.mean, self.streak, self.bad_acts = 0.0, 0, 0

    def score(self) -> float:
        self._fade()
        return self.nice - self.mean

    def good(self, why: str, ed: "Edith"):
        self._fade()
        self.nice += 1
        self.streak = 0
        ed.log(f"  {self.name}: +1 nice ({why}) -> nice {self.nice}, mean {self.mean:.1f}, score {self.score():+.1f}")

    def bad(self, why: str, ed: "Edith", cancel_after: int = 3) -> bool:
        self._fade()
        self.mean += 1
        self._set("mean_at", time.time())
        self.streak += 1
        wiped = self.streak >= cancel_after and self.nice > 0
        if wiped:
            self.nice, self.streak = 0, 0
        ed.log(f"  {self.name}: +1 mean ({why}) -> nice {self.nice}, mean {self.mean:.1f}, score {self.score():+.1f}"
               + (" -- nice points wiped" if wiped else ""))
        return wiped


PEOPLE: Dict[str, Person] = {}


def person(name: str) -> Person:
    p = PEOPLE.get(name)
    if p is None:
        p = PEOPLE[name] = Person(name)
    return p


# ---- game events ---------------------------------------------------------------------------------

def classify(text: str):
    """Satellite.dll event -> (reaction name, memory text)."""
    if text == "grabbed":
        return "grabbed", "you grabbed her"
    if text.startswith("released"):
        if text.endswith("thrown"):
            return "thrown", "you threw her"
        return "released", "you let her go"
    if text == "headbutt":
        return "headbutt", "you headbutted her"
    if text == "bumped":
        return "bumped", "she got bumped"
    if text == "hit wall":
        return "hit wall", ""
    if text.startswith("threw snowball (revenge)"):
        return "threw snowball", "she threw a snowball at you"
    if text.startswith("threw snowball"):
        return None, "she threw a snowball at you (you asked)"
    if text.startswith("pushed player"):
        return None, "she shoved you"
    if text.startswith("shut player down"):
        return None, "she shut your echo unit down"
    if text.startswith("dizzy") or text.startswith("stopped spinning"):
        return None, ""
    return None, text


#: her face for each reaction (Satellite.dll "face <mood> <seconds>")
FACES = {"grabbed": ("surprised", 4), "thrown": ("hurt", 5), "headbutt": ("dizzy", 6), "bumped": ("annoyed", 4),
         "hit wall": ("hurt", 3), "released": ("happy", 0), "threw snowball": ("angry", 4)}

#: short commands she carries out at once: (order, pattern); her lines are lines.json order/<order>
INSTANT = [
    ("follow", r"\b(follow\w*|come with me|with me)\b"),
    ("come", r"\b(come (here|over|back|to me)|over here|get over here|come)\b"),
    ("home", r"\b(go home|home|spawn|go back to (the )?(start|middle|centre|center))\b"),
    ("stay", r"\b(stay|stop|wait|hold on|freeze|stay there)\b"),
    ("spin", r"\b(spin|twirl|barrel roll|do a flip)\b"),
    ("throw", r"\b(throw|snowball|hit me|balls?)\b"),
]
NUMBERS = {"one": 1, "once": 1, "two": 2, "twice": 2, "three": 3, "four": 4, "five": 5, "six": 6, "ten": 10}
SPOT_NAMED = r"\b(?:as|to be|called|named|is)\b"
TEACH = re.compile(r"^(?:when|if|whenever) (?:i|someone|anyone|somebody|people|a player|they|he|she) "
                   r"(?:say|says|ask|asks|tell|tells)(?: you)? (.+?),? (?:then )?(?:you )?(?:should |can |could )?"
                   r"(?:say|answer|reply|respond|tell (?:them|me|him|her))(?: with)? (.+)$", re.I)
ANSWER = re.compile(r"^(?:you (?:should|could|can) (?:say|answer|reply|respond)(?: with)?|(?:just )?say|the answer is|"
                    r"answer(?: with)?|reply(?: with)?|respond with|tell (?:them|me|him|her)) (.+)$", re.I)
UNLEARN = re.compile(r"^(?:forget|unlearn) (?:what you say (?:to|when (?:i|someone) (?:say|says|ask|asks))|"
                     r"the answer (?:to|for)|your answer (?:to|for)) (.+)$", re.I)


def parse_players(state: str) -> Dict[str, List[float]]:
    """Everyone's position from a state line: players=<name>:x,y,z;<name>:x,y,z;... (EDITH.dll with
    roam / approach). {} when the game doesn't report them yet."""
    m = re.search(r"\bplayers=(\S*)", state)
    out: Dict[str, List[float]] = {}
    for item in (m.group(1).split(";") if m else []):
        name, _, xyz = item.rpartition(":")
        try:
            pos = [float(v) for v in xyz.split(",")]
        except ValueError:
            continue
        if name and len(pos) == 3 and all(math.isfinite(v) for v in pos):
            out[name] = pos
    return out


SORRY = re.compile(r"\b(sorry|apologi[sz]e\w*|apologies|forgive me)\b")
#: words that don't make "sorry" an apology: "oops sorry", "sorry lol", "my bad" (user, 2026-10-19)
SORRY_FILLER = set("""sorry so oh ok okay hey uh um umm yo well oops whoops my bad fault lol haha lmao bro dude man
mate please ah ahh sry soz""".split())


def apology(text: str, named: bool) -> Optional[str]:
    """"real": sorry said to her by name plus something more ("I'm really sorry Edith", "Edith,
    I'm sorry for throwing you"); "bare": a sorry / my bad / oops without that; None: no apology.
    `text` is the line without her name (strip_wake); `named`: her name was in it."""
    t = norm(text)
    if not re.search(r"\b(sorry|apologi[sz]e\w*|apologies|forgive me|my bad|oops|whoops|my fault|sry|soz)\b", t):
        return None
    more = [w for w in t.split() if w.strip("'") not in SORRY_FILLER]
    return "real" if named and SORRY.search(t) and more else "bare"


def instant(text: str):
    """A short line (up to 8 words) asking for an order -> order name; with two, the one said first."""
    t = text.lower().strip(" .!?,")
    if len(t.split()) > 8 or "?" in text:
        return None
    hits = [(re.search(pat, t), o) for o, pat in INSTANT]
    hits = sorted((m.start(), o) for m, o in hits if m)
    return hits[0][1] if hits else None


def is_question(text: str) -> bool:
    t = text.lower().strip()
    return "?" in t or bool(re.match(r"(what|whats|what's|who|whos|who's|how|when|where|why|which|is|are|was|were|do|does|did|"
                                      r"can|could|tell me|explain|describe|define)\b", t))


def strip_wake(cfg: dict, text: str):
    t = text.lower()
    for w in cfg["wake_words"]:
        m = re.search(rf"\b{re.escape(w)}\b[\s,.!?]*", t)
        if m:
            return (text[:m.start()] + text[m.end():]).strip(" ,.!?") or "hi"
    return None


def no_learn(line: str, who: Optional[str], me: str, members: list) -> bool:
    """A line naming someone is about them: not an answer for everyone."""
    names = {n.lower() for n in [who or "", me or ""] + list(members) if n and n != "you"}
    low = line.lower()
    return any(n in low for n in names)


def prime(name: str) -> str:
    n = name
    return (f"{n}, throw a ball at me. {n}, follow me. Come here, {n}. Stay there. Spin. Keep spinning until I say so. "
            f"Do a front flip. Do a back flip. Stop. Push me. Shut me down. "
            f"Go back to spawn. Go home. Throw a snowball. {n}, what are you doing? Echo Arena, Echo Combat, "
            f"Lone Echo, joust, regrab, stun.")


# ---- one Edith: the one a companion's game runs -----------------------------------------------------

class Edith:
    """The Edith in one player's game (their companion's connection). Her state in the match comes
    from Satellite.dll's state line; who she knows (points, profiles) is shared by name; her own
    memory (events, places, what she was taught) is the record of the player whose game runs her."""

    def __init__(self, sess: "Session"):
        self.sess = sess
        self.state, self.state_time = "", 0.0
        self.me, self.members, self.role, self.with_ai = "", [], "", set()
        self.quiet_until = 0.0
        self.last_push = self.last_shutdown = 0.0
        self.last_unknown, self.last_unknown_at = "", 0.0
        self.last_move = "spin"
        self.spinning = False
        self.order_by, self.order_at = "", 0.0
        self.favour, self.favour_at = None, 0.0
        self.forwarded: Dict[str, float] = {}
        self.muted, self.muted_at = False, 0.0
        self.speaking_until = 0.0
        self.picks: dict = {}
        self.last_reaction = 0.0
        self.tz_min = 0
        self.anon = blank_record()                                 # her memory until she knows who you are
        self.learned_src, self.learned_pool, self.learned_lib = None, [], None
        self._orders: List[str] = []
        self._face = None
        self.last_auto = None                                      # (self-learned answer id, when, who)
        self.spawned = False                                       # the DLL said you're in her lobby
        # her own initiative: everyone's position from the state line (players=, EDITH.dll with roam),
        # when someone last talked to her, when she last took the lead and with whom
        self.positions: Dict[str, List[float]] = {}
        self.her_pos: Optional[List[float]] = None
        self.last_heard = time.time()
        self.last_initiative = time.time()
        self.opened = ("", 0.0)

    # -- plumbing --
    @property
    def lib(self) -> Library:
        return LIB.get()

    @property
    def cfg(self) -> dict:
        return self.lib.cfg

    def log(self, *a):
        text = " ".join(str(x) for x in a)
        log(f"[{self.me or self.sess.hello_name or '?'}]", text)
        self.sess.send({"t": "log", "text": text})

    def lt(self):
        """Now, in the player's own time zone (events are dated in it, {time} says it)."""
        return time.gmtime(time.time() + self.tz_min * 60)

    def stamp(self) -> str:
        return time.strftime("%Y-%m-%d %H:%M", self.lt())

    def busy(self) -> bool:
        """She is talking (the companion says when her line has played)."""
        return time.time() < self.speaking_until

    def order(self, o: str):
        self._orders.append(o)

    def face(self, mood: str, seconds: float = 0):
        self._face = [mood, seconds]

    def flush(self):
        """Orders / a face without a line."""
        if self._orders or self._face:
            self.sess.send({"t": "reply", "say": "", "orders": self._orders, "face": self._face, "how": ""})
            self._orders, self._face = [], None

    def voice(self, say: str, how: str):
        """Her line, with its voice pack keys, plus whatever orders / face go with it."""
        c = self.cfg
        v, fb, sp = c["voice"], c["fallback_voice"], c["voice_speed"]
        if say and self.role == "puppet":                              # the host's Edith speaks for everyone:
            self.log(f"  (puppet: not voiced here, the host's Edith talks) {say!r}")   # her lines reach puppets
            say = ""                                                   # through the relay (EDITH.dll plays them)
        msg = {"t": "reply", "say": say, "orders": self._orders, "face": self._face, "how": how}
        if say:
            msg["key"], msg["alt"] = voice_key(v, sp, say), voice_key(fb, sp, say)
            keys = [msg["key"], msg["alt"]]
            parts = sentences(say)
            if len(parts) > 1:
                msg["parts"] = [[p, voice_key(v, sp, p), voice_key(fb, sp, p)] for p in parts]
                keys += [k for part in msg["parts"] for k in part[1:]]
            if self.lib.must_stream(keys):                             # a broken bank copy: the game
                msg["stream"] = True                                   # streams the server's render
            self.speaking_until = time.time() + 30.0                   # till the companion says it's done
        self.sess.send(msg)
        self._orders, self._face = [], None

    # -- her memory (the record of whoever's game runs her) --
    @property
    def rec(self) -> dict:
        return store().get(self.me) if self.me else self.anon

    def save(self):
        if self.me:
            store().mark(self.me)

    def prof(self, who: Optional[str]) -> dict:
        """What `who` told her about themself (their own record)."""
        name = who or self.me
        if not name or name == "you":
            return self.anon["profile"]
        return store().get(name)["profile"]

    def save_prof(self, who: Optional[str]):
        name = who or self.me
        if name and name != "you":
            store().mark(name)

    def event(self, text: str):
        ev = self.rec["events"]
        ev.append(f"{self.stamp()} {text}")
        del ev[:-500]
        self.save()

    def count_today(self, prefix: str) -> int:
        today = time.strftime("%Y-%m-%d", self.lt())
        return sum(1 for e in self.rec["events"] if e.startswith(today) and e[17:].startswith(prefix))

    def turn(self, who: str, text: str):
        t = self.rec["turns"]
        t.append([who, text])
        del t[:-16]
        self.save()

    def learned(self) -> list:
        """What she was taught, as a chat pool (rebuilt when it or the library changes)."""
        lr = self.rec.get("learned", [])
        sig = json.dumps(lr)
        if sig != self.learned_src or self.learned_lib is not self.lib:
            self.learned_src, self.learned_lib = sig, self.lib
            self.learned_pool = [(e, self.lib.examples(e["q"])) for e in lr]
        return self.learned_pool

    def player_pos(self):
        m = re.search(r"player=([-\d.]+),([-\d.]+),([-\d.]+)", self.state)
        return [float(m.group(i)) for i in (1, 2, 3)] if m else None

    def in_lobby(self) -> bool:
        return time.time() - self.state_time < 6.0

    def mark(self, place: str) -> bool:
        pos = self.player_pos()
        if not pos or not place.strip():
            return False
        places = self.rec.setdefault("places", {})
        places.pop(place.strip().lower(), None)
        places[place.strip().lower()] = pos
        self.save()
        return True

    def place(self, name: str):
        n = re.sub(r"\b(the|area|spot|place|room)\b", "", name.lower()).strip(" .,!?")
        places = dict(self.rec.get("places", {}))
        if n in ("", "there", "that", "it", "my", "our", "same", "that same", "last") and places:
            k = list(places)[-1]
            return k, places[k]
        for k, v in places.items():
            kk = re.sub(r"\b(the|area|spot|place|room)\b", "", k).strip()
            if n and (n == kk or n.rstrip("s") == kk.rstrip("s") or (len(n) >= 4 and (n in kk or kk in n))):
                return k, v
        best = max(places, key=lambda k: SequenceMatcher(None, name.lower(), k).ratio(), default=None)
        if best and SequenceMatcher(None, name.lower().strip(" .,!?"), best).ratio() >= 0.75:   # misheard
            return best, places[best]
        return None

    def forget_place(self, name: str):
        hit = self.place(name)
        if not hit:
            return None
        self.rec["places"].pop(hit[0], None)
        self.save()
        return hit[0]

    def teach(self, said: str, answer: str):
        said, answer = said.strip(" .!?,"), answer.strip(" ,")
        key = re.sub(r"[^a-z0-9 ]", "", said.lower()).strip()
        lr = self.rec.setdefault("learned", [])
        for e in lr:
            if key in [re.sub(r"[^a-z0-9 ]", "", q.lower()).strip() for q in e["q"]]:
                if answer not in e["a"]:
                    e["a"].append(answer)
                break
        else:
            e = {"id": f"learned_{len(lr) + 1}", "q": [said], "a": [answer], "at": time.strftime("%Y-%m-%d", self.lt())}
            lr.append(e)
        self.save()
        return e

    def unlearn(self, about: str) -> int:
        w = set(re.findall(r"[a-z0-9]+", about.lower())) - {"the", "a", "to", "what", "you", "say", "i"}
        lr = self.rec.get("learned", [])
        keep = [e for e in lr if not any(w and w <= set(re.findall(r"[a-z0-9]+", q.lower())) for q in e["q"])]
        self.rec["learned"] = keep
        self.save()
        return len(lr) - len(keep)

    # -- a player's profile --
    def add(self, who, kind: str, item: str):
        item = item.strip(" .!?,")
        if not item:
            return
        p = self.prof(who)
        lst = p[kind]
        if item.lower() not in [x.lower() for x in lst]:
            lst.append(item)
            del lst[:-100]
        other = {"likes": "dislikes", "dislikes": "likes"}.get(kind)
        if other:
            p[other] = [x for x in p[other] if x.lower() != item.lower()]
        self.save_prof(who)

    def forget(self, who, what: str) -> int:
        w = set(re.findall(r"[a-z0-9]+", what.lower())) - {"the", "a", "that", "my", "i", "about"}
        if not w:
            return 0
        n = 0
        p = self.prof(who)
        for kind in ("likes", "dislikes", "notes"):
            keep = [x for x in p[kind] if not (w & set(re.findall(r"[a-z0-9]+", x.lower())))]
            n += len(p[kind]) - len(keep)
            p[kind] = keep
        for k in [k for k in p["favorites"] if w & set(k.split())]:
            del p["favorites"][k]
            n += 1
        if self.is_me(who):
            facts = [f for f in self.rec["facts"] if not (w & set(re.findall(r"[a-z0-9]+", f.lower())))]
            n += len(self.rec["facts"]) - len(facts)
            self.rec["facts"] = facts
            self.save()
        self.save_prof(who)
        return n

    def recall(self, who, about: str, need: int = 1):
        w = set(re.findall(r"[a-z0-9]+", about.lower())) - {"the", "a", "that", "my", "i", "about", "what", "remember"}
        p = self.prof(who)
        best, hit = 0, None
        facts = self.rec["facts"] if self.is_me(who) else []
        for kind, items in (("note", p["notes"]), ("like", p["likes"]), ("dislike", p["dislikes"]), ("fact", facts)):
            for x in items:
                s = len(w & set(re.findall(r"[a-z0-9]+", x.lower())))
                if s > best:
                    best, hit = s, (kind, x)
        for k, v in p["favorites"].items():
            s = len(w & set(k.split())) + len(w & set(v.lower().split()))
            if s > best:
                best, hit = s, ("favorite", f"your favorite {k} is {v}")
        if not hit or best < need:
            return None
        kind, x = hit
        return {"like": f"you like {x}", "dislike": f"you don't like {x}"}.get(kind, x)

    # -- who is who --
    def person(self, who: Optional[str] = None) -> Person:
        return person(who or self.me or "you")

    def score(self, who: Optional[str] = None) -> float:
        return self.person(who).score()

    def is_me(self, who: Optional[str]) -> bool:
        return not who or who == self.me or (not self.me and who == "you")

    def as_who(self, text: str, who: Optional[str]) -> str:
        return text if self.is_me(who) or not text else re.sub(r"\byou\b", who, text)

    def favourite(self) -> str:
        names = [n for n in self.members if n] or [self.me or "you"]
        return min(names, key=lambda n: (-round(self.score(n), 1), -self.person(n).last_spoke, not self.is_me(n)))

    def tell_favourite(self):
        if not self.me:
            return
        f = self.favourite()
        if f != self.favour or time.time() - self.favour_at > 10:
            if f != self.favour:
                self.log(f"favourite: {f} ({', '.join(f'{n} {self.score(n):+.1f}' for n in (self.members or [f]))})")
            self.order(f"favour {f if f != 'you' else ''}".strip())
            self.favour, self.favour_at = f, time.time()

    def fill(self, line: str) -> str:
        line = drop_name(line).replace("{me}", self.cfg["name"])
        lt = self.lt()
        line = line.replace("{time}", time.strftime("%I:%M %p", lt).lstrip("0"))
        return line.replace("{date}", time.strftime("%A, %B %d", lt).replace(" 0", " "))

    def gate(self, say: str, how: str) -> str:
        """The barrier's last word on any line of hers (edith_barrier.py): blocked, she says something else."""
        why = self.lib.barrier.screen(say) if say else None
        if not why:
            return say
        self.log(f"  barrier: blocked her line ({why}, {how}): {say[:80]!r}")
        return say_clean(Library.pick(self.picks, "screened", self.lib.line("screened", ["Let's talk about something else."])))

    def speak(self, say: str, how: str, t_heard: Optional[float] = None):
        say = self.gate(say_clean(self.fill(say)), how)
        after = f", {time.time() - t_heard:.2f} s after they stopped" if t_heard else ""
        self.log(f"{self.cfg['name']} ({how}{after}): {say!r}")
        self.turn("her", say)
        self.voice(say, how)

    def aside(self, line: str, how: str):
        """A reaction line: only when she isn't talking already."""
        if line and not self.busy():
            line = self.fill(line)
            line = self.gate(say_clean(line), how)
            self.log(f"{self.cfg['name']} ({how}): {line!r}")
            self.voice(line, how)

    # -- Satellite.dll's lines (via the companion) --
    def read_state(self, line: str):
        self.state, self.state_time = line, time.time()
        m = re.search(r"\bme=(\S*)", line)
        if m and m.group(1) and m.group(1) != self.me:
            self.me = m.group(1)
            self.log(f"this game's player: {self.me}")
            if self.spawned:
                self.tell_reports()
        m = re.search(r"\bnet=(\S*)", line)
        self.role = m.group(1) if m else ""
        m = re.search(r"\bmembers=(\S*)", line)
        self.members = [n for n in (m.group(1).split(",") if m else []) if n]
        m = re.search(r"\bai=(\S*)", line)
        self.with_ai = {n for n in (m.group(1).split(",") if m else []) if n}
        self.positions = parse_players(line)
        m = re.search(r"\bher=([-\d.]+),([-\d.]+),([-\d.]+)", line)
        self.her_pos = [float(m.group(i)) for i in (1, 2, 3)] if m else None

    def on_dll(self, msg: str):
        """A line from Satellite.dll's event port (ai/satellite_ai.py listen_events)."""
        if msg.startswith("state "):
            self.read_state(msg[6:])
            self.tell_favourite()
            self.initiative()
        elif msg.startswith("mute "):                                  # "mute 0|1 me=<name> speaker=0|1"
            m = re.search(r"\bme=(\S+)", msg)                          # (your client says who you are: a
            if m and m.group(1) != self.me:                            #  server's state lines don't know)
                self.me = m.group(1)
                self.log(f"this game's player: {self.me}")
            muted = msg[5:].split()[0] == "1" if msg[5:].split() else False
            if muted != self.muted:
                self.log("muted on voice chat: not listening to the mic" if muted else "unmuted: listening to the mic")
            self.muted, self.muted_at = muted, time.time()
        elif msg.startswith("heard "):                                 # another player's companion heard them
            parts = msg[6:].split(" ", 1)
            if len(parts) == 2 and parts[0]:
                self.forwarded[parts[0]] = time.time()
                self.on_heard(parts[1], parts[0], time.time())
        elif msg.startswith("event "):
            self.on_event(msg[6:])
        elif msg.startswith("error "):                                 # "error no player <name>" (approach)
            self.log("game error:", msg[6:])
            m = re.match(r"error no player (\S+)", msg)
            if m and self.opened[0] == m.group(1):
                self.opened = ("", 0.0)
                self.last_initiative = 0.0                             # pick someone else next state line
        elif msg == "spawned":                                         # you're in her lobby (EDITH.dll)
            self.spawned = True
            self.tell_reports()
        self.flush()

    def on_event(self, text: str):
        c, lib = self.cfg, self.lib
        m = re.search(r"\s+by=(\S*)", text)
        who = (m.group(1) or None) if m else None
        m = re.search(r"\s+to=(\S*)", text)
        to = m.group(1) if m else ""
        text = re.sub(r"\s+(?:by|to)=\S*", "", text).strip()
        self.log("game:", text, f"(by {who})" if who else "", f"(to {to})" if to else "")
        if text == "approached" or text.startswith("lost "):          # her approach (initiative): arrived /
            target = to or text[5:].strip()                            # the player left before she got there
            if text != "approached" and self.opened[0] == target:
                self.opened = ("", 0.0)
            return
        if not self.modded(who):                                       # someone without the mod bumped her
            self.log(f"  ({who} doesn't run E.D.I.T.H: ignored)")
            return
        react, remember = classify(text)
        if remember:
            self.event(self.as_who(remember, who))
        m = re.match(r"(dizzy|stopped spinning) t=(\d+)", text)
        if m:
            self.face("dizzy", 4)
        elif react in FACES:
            self.face(*(("angry", 6) if react in ("thrown", "headbutt") and self.score(who) < 0 else FACES[react]))
        if m:
            if time.time() > self.quiet_until:
                line = lib.dizzy_line(self.picks, int(m.group(2))) if m.group(1) == "dizzy" else lib.mood(self.picks, "stopped spinning")
                self.aside(line or "", text)
            return
        if self.judge_act(react or "", who):
            return
        if react and c.get("react", True) and not self.busy() and time.time() > self.quiet_until:
            if react in ("thrown", "headbutt", "grabbed", "bumped") and self.sorry_due(self.person(who)):
                return self.aside(Library.pick(self.picks, "ask_sorry", lib.line("ask_sorry")), f"{react}: asks for an apology")
            line = lib.reaction(self, react, self.count_today(self.as_who(remember, who)))
            self.aside(line or "", f"reacts to {react}")

    def judge_act(self, react: str, who: Optional[str]) -> bool:
        """Points for what someone did to her, and her answer when it's bad (see satellite_ai.py)."""
        c, lib = self.cfg, self.lib
        p = self.person(who)
        to = f" to={p.name}" if p.name != "you" else ""
        if react == "grabbed" and p.score() > 0:
            if not self.busy():
                self.face("happy", 4)
                self.aside(Library.pick(self.picks, "nice_hold", lib.line("nice_hold")), f"held by a friend, {p.name}")
            return True
        if react not in ("thrown", "headbutt", "grabbed", "bumped"):   # (bumps too: "hit her with things", user)
            return False
        wiped = p.bad(react, self, c.get("cancel_after", 3))
        self.tell_favourite()
        if not c.get("mean", True):
            return False
        now = time.time()
        p.bad_acts += 1
        if p.score() <= -c.get("shutdown_after", 5):
            p.bad_acts = 0
            p.mean = max(0.0, p.mean - 3)
            self.last_shutdown = now
            self.order("shutdown" + to)
            self.face("angry", 10)
            self.speak(lib.mood(self.picks, "angry shutdown") or "", f"{p.name} at {p.score():+.1f}: shutdown")
            return True
        if wiped:
            p.last_snow = now
            self.order("throw" + to)
            self.face("angry", 8)
            self.speak(Library.pick(self.picks, "mean_back", lib.line("mean_back")), f"{p.name}'s nice points wiped: snowball")
            return True
        if p.score() < 0 and p.bad_acts >= c.get("push_after", 2):
            p.bad_acts = 0
            self.last_push = now
            self.order("push 9" + to)
            self.face("angry", 8)
            self.speak(lib.mood(self.picks, "angry push") or "", f"{p.name} at {p.score():+.1f}: push")
            return True
        return False

    # -- orders --
    def aim(self, order: str, who: Optional[str]) -> str:
        name = who or self.me
        if not name or name == "you" or "to=" in order:
            return order
        if order in ("follow", "come"):
            return f"{order} {name}"
        if order.split()[0] in ("throw", "push", "shutdown"):
            return f"{order} to={name}"
        return order

    def carry_out(self, acts: list, place: str = "", who: Optional[str] = None) -> list:
        done = []
        for a in acts:
            if a == "mark":
                if self.mark(place):
                    done.append(f"mark {place}")
                    self.event(self.as_who(f"you showed her the {place}", who))
            elif a == "goto":
                hit = self.place(place)
                if hit:
                    self.order("goto %.2f %.2f %.2f" % tuple(hit[1]))
                    done.append(f"goto {hit[0]}")
            else:
                self.order(self.aim(a, who))
                done.append(a)
                if a.startswith(("spin", "flip")):
                    self.last_move = a
        return done

    def refusal(self, who: Optional[str], order: str = "", said: str = ""):
        """She takes orders from everyone, but not equally (see satellite_ai.py refusal())."""
        c, lib = self.cfg, self.lib
        p = self.person(who)
        sc = p.score()
        now = time.time()
        ob = self.order_by
        if ob and ob != p.name and now - self.order_at < c.get("order_hold_s", 60) and self.score(ob) > sc:
            self.log(f"  refused {p.name} ({sc:+.1f}): busy with {ob} ({self.score(ob):+.1f})")
            return Library.pick(self.picks, "refuse_busy", lib.line("refuse_busy")), None
        others = [n for n in self.members if n and n != p.name]
        if others and sc <= -c.get("ignore_below", 5.0) and any(self.score(n) > sc for n in others):
            self.log(f"  refused {p.name} ({sc:+.1f}): too mean, and someone nicer is here")
            return Library.pick(self.picks, "refuse_mean", lib.line("refuse_mean")), None
        if sc < 0:
            if (p.asked == order and now - p.asked_at < 20) or re.search(r"\bplease\b", said, re.I):
                p.asked = ""
                self.log(f"  {p.name} ({sc:+.1f}) asked again: done, grumbling")
                return None, Library.pick(self.picks, "grudging", lib.line("grudging"))
            p.asked, p.asked_at = order, now
            self.log(f"  stubborn with {p.name} ({sc:+.1f}): ask again")
            key = "ask_sorry" if self.sorry_due(p) else "stubborn"
            return Library.pick(self.picks, key, lib.line(key)), None
        return None, None

    def obey(self, who: Optional[str]):
        self.order_by, self.order_at = self.person(who).name, time.time()

    def move_instant(self, text: str):
        """Spins, flips, keep going, stop, push and shutdown -> (order, her line)."""
        lib = self.lib
        t = text.lower().strip(" .!?,")
        if len(t.split()) > 12:
            return None
        keep = bool(re.search(r"\b(keep (going|spinning|flipping|back ?flipping|front ?flipping|doing (it|that)|it up)|"
                              r"don'?t stop|until i (say|tell you)|till i (say|tell you)|"
                              r"forever|non ?stop|endless(ly)?)\b", t))
        if re.search(r"^(ok(ay)? |alright |now )?(stop|enough|that'?s enough|you can stop|stop (it|spinning|flipping|now)|quit it)$", t) \
                or (self.spinning and not keep and re.search(r"\b(stop|enough|halt)\b", t)):
            if self.spinning:
                self.spinning = False
                return "stop", Library.pick(self.picks, "move/stop", lib.line("move/stop"))
            return None
        move = None
        if re.search(r"\b(back ?flip(s|ping)?|backwards flips?|backflood)\b", t):
            move = "flip back"
        elif re.search(r"\b(front ?flip(s|ping)?|forward flips?|flip(s|ping)?)\b", t):
            move = "flip front"
        elif re.search(r"\b(barrel roll|side ?flips?|cartwheels?|roll)\b", t):
            move = "flip right" if "right" in t else "flip left"
        elif re.search(r"\b(spin|spinning|twirl)\b", t):
            move = "spin"
        elif keep and re.search(r"\b(going|doing (it|that)|it up|until|till)\b", t):
            move = self.last_move
        if move:
            n = re.search(r"\b(\d+|one|once|two|twice|three|four|five|six|ten)\b", t)
            turns = (int(n.group(1)) if n.group(1).isdigit() else NUMBERS[n.group(1)]) if n else 1
            self.last_move = move
            self.spinning = keep
            order = f"{move} keep" if keep else (f"{move} {turns}" if turns > 1 else move)
            key = "move/keep" if keep else "move/" + move
            return order, Library.pick(self.picks, key, lib.line(key))
        if re.search(r"\b(shut (me|my (echo )?unit|my echo) down|shoot me down|shutdown|shut down|kill me|respawn me|reset me|"
                     r"power me (off|down))\b", t):
            self.last_push = time.time()
            return "push", self.fill(lib.mood(self.picks, "asked shutdown") or "")
        throw_me = re.search(r"\bthrow me\b", t) and not re.search(r"\bthrow me (a|an|one|some|your|another|that|this)\b|snow", t)
        if re.search(r"\b(push|shove|knock|launch|yeet|fling|bump|toss) me\b", t) or throw_me:
            hard = re.search(r"\b(hard|harder|really|far|max)\b", t)
            self.last_push = time.time()
            return ("push 12" if hard else "push"), self.fill(lib.mood(self.picks, "asked push") or "")
        return None

    # -- memory and teaching by voice --
    def memory_instant(self, who, text: str):
        """Remember / recall: each player's own profile."""
        lib = self.lib
        t = re.sub(r"^((?:cubie|edith),?\s*|hey\s+|please\s+|can you\s+|could you\s+)+", "", text.strip(), flags=re.I).strip(" .!?,")
        lo = t.lower()
        p = self.prof(who)
        m = re.match(r"(?:call me|my name is|i'm called|im called|you can call me)\s+(.+)$", lo)
        if m:
            nick = t[-len(m.group(1)):].strip(" .!?,").title()
            p["nickname"] = nick
            self.save_prof(who)
            self.event(self.as_who(f"you told her to call you {nick}", who))
            return Library.pick(self.picks, "nickname", lib.line("nickname"))
        m = re.match(r"my (?:favou?rite|fave) ([a-z ]{2,30}?) (?:is|are) (.+)$", lo)
        if m:
            thing, val = m.group(1).strip(), t[-len(m.group(2)):].strip(" .!?,")
            p["favorites"][thing] = val
            self.save_prof(who)
            return f"{val}, your favorite {thing}. Got it!"
        m2 = re.match(r"(.+?),?\s+(?:remember that|remember it|don'?t forget (?:that|it)|got it)$", lo)
        if m2 and not re.search(r"\b(this|here|spot|place)\b", m2.group(1)):   # "the capital of france is paris, remember that"
            note = you_form(t[:len(m2.group(1))].strip(" .!?,"))
            self.add(who, "notes", note)
            return f"Got it: {note}."
        if re.match(r"(what'?s|what is|what was) the first thing you (said|told me)$", lo):
            first = next((x for w, x in self.rec.get("turns", []) if w == "her"), None)
            return f"First thing I said? {first}" if first else "I don't remember. I talk a lot."
        m = re.match(r"(?:remember|don't forget|dont forget|note) (?:that )?(.+)$", lo)
        if m and not re.search(r"\b(this|here|spot|place)\b.*" + SPOT_NAMED, lo):
            note = you_form(t[-len(m.group(1)):].strip(" .!?,"))
            self.add(who, "notes", note)
            return f"Got it: {note}."
        m = re.match(r"i (?:really )?(?:like|love|enjoy|adore) (?!you\b)(.+)$", lo)
        if m and len(m.group(1).split()) <= 6:
            thing = t[-len(m.group(1)):].strip(" .!?,")
            self.add(who, "likes", thing)
            return random.choice([f"You like {thing}? Noted.", f"{thing}, good taste.", f"I'll remember you like {thing}."])
        m = re.match(r"i (?:really )?(?:hate|don't like|dont like|can't stand|cant stand|dislike) (?!you\b)(.+)$", lo)
        if m and len(m.group(1).split()) <= 6:
            thing = t[-len(m.group(1)):].strip(" .!?,")
            self.add(who, "dislikes", thing)
            return random.choice([f"No {thing} then. Noted.", f"Got it, you don't like {thing}."])
        m = re.match(r"forget (?:that |about )?(.+)$", lo)
        if m:
            what = m.group(1)
            if what in ("everything", "everything about me", "all of it"):
                p.update({"nickname": "", "likes": [], "dislikes": [], "favorites": {}, "notes": []})
                if self.is_me(who):
                    self.rec["facts"] = []
                    self.save()
                self.save_prof(who)
                return "Done. Who are you again? Kidding. Mostly."
            n = self.forget(who, what)
            return "Forgotten." if n else "I didn't have anything about that."
        if re.match(r"(what's|whats|what is|do you know|say) my name\??$", lo):
            return Library.pick(self.picks, "my_name", lib.line("my_name"))
        if re.match(r"(what do you (know|remember)( about me)?|what have you learned about me|what do you remember)$", lo):
            bits = []
            if p["likes"]:
                bits.append("you like " + ", ".join(p["likes"][-3:]))
            if p["dislikes"]:
                bits.append("you don't like " + p["dislikes"][-1])
            if p["favorites"]:
                k, v = list(p["favorites"].items())[-1]
                bits.append(f"your favorite {k} is {v}")
            if p["notes"]:
                bits.append(p["notes"][-1])
            return ("I know " + "; ".join(bits[:4]) + ".") if bits else "Not much yet. Tell me things!"
        if re.match(r"what do i (like|love|enjoy)$", lo):
            return ("You like " + ", ".join(p["likes"][-5:]) + ".") if p["likes"] else "You haven't told me yet."
        if re.match(r"what do i (hate|dislike|not like)$", lo):
            return ("You don't like " + ", ".join(p["dislikes"][-5:]) + ".") if p["dislikes"] else "Nothing, apparently. Except bad throws."
        m = re.match(r"(?:what's|whats|what is) my (?:favou?rite|fave) (.+)$", lo)
        if m:
            k = m.group(1).strip()
            for fk, v in p["favorites"].items():
                if fk == k or fk in k or k in fk:
                    return f"Your favorite {fk} is {v}."
            return f"You never told me your favorite {k}."
        m = re.match(r"do you remember (.+)$", lo)
        if m:
            hit = self.recall(who, m.group(1))
            return f"Of course: {hit}." if hit else "Hmm, no. Tell me again?"
        m = re.match(r"how many times (?:did|have) (?:i|you) (?:been )?(throw|thrown|threw|bump|bumped|grab|grabbed|headbutt|headbutted)(?: you| me)?(?: today)?$", lo)
        if m:
            key = {"throw": "you threw her", "thrown": "you threw her", "threw": "you threw her", "bump": "she got bumped",
                   "bumped": "she got bumped", "grab": "you grabbed her", "grabbed": "you grabbed her",
                   "headbutt": "you headbutted her", "headbutted": "you headbutted her"}[m.group(1)]
            n = self.count_today(self.as_who(key, who))
            return f"{n} times today. I'm counting." if n != 1 else "Once today. I'm counting."
        if re.match(r"what (did we do|happened|have we done)( today)?$", lo):
            today = time.strftime("%Y-%m-%d", self.lt())
            kinds: Dict[str, int] = {}
            for e in self.rec["events"]:
                if e.startswith(today):
                    k = e[17:].split(" speed")[0]
                    if not k.startswith("you said") and " said " not in k:
                        kinds[k] = kinds.get(k, 0) + 1
            if not kinds:
                return "Not much yet. Let's do something!"
            top = sorted(kinds.items(), key=lambda x: -x[1])[:3]
            times = lambda n: "once" if n == 1 else "twice" if n == 2 else f"{n} times"
            mine = lambda k: re.sub(r"\b(her|she)\b", lambda w: "me" if w.group(1) == "her" else "I", k)
            return "Today " + ", ".join(f"{mine(k)} {times(n)}" for k, n in top) + "."
        if re.match(r"(what|which) places do you know$|where can you go$", lo):
            names = list(self.rec.get("places", {}))
            return ("I know the " + ", the ".join(names[-5:]) + ".") if names else "No saved places yet. Say remember this spot as, and a name."
        return None

    def teach_instant(self, heard: str):
        """Teaching her (the player whose game runs her): see satellite_ai.py teach_instant()."""
        lib = self.lib
        t = re.sub(r"^(?:(?:cubie|edith),?\s*|hey\s+|okay\s+|ok\s+|so\s+)+", "", heard.strip(), flags=re.I).strip(" .!?")
        m = TEACH.match(t) or ANSWER.match(t)
        if m and (lib.barrier.screen(m.group(m.lastindex)) or lib.barrier.check_in(m.group(1))):
            self.log("  barrier: won't learn that")
            return Library.pick(self.picks, "refuse_bigot", lib.line("refuse_bigot"))
        m = TEACH.match(t)
        if m:
            self.teach(m.group(1), m.group(2))
            self.event(f"you taught her to answer \"{m.group(1)[:40]}\"")
            return Library.pick(self.picks, "taught", lib.line("taught"))
        m = ANSWER.match(t)
        if m and self.last_unknown and time.time() - self.last_unknown_at < 60:
            self.teach(self.last_unknown, m.group(1))
            self.event(f"you taught her to answer \"{self.last_unknown[:40]}\"")
            self.last_unknown = ""
            return Library.pick(self.picks, "taught_answer", lib.line("taught_answer"))
        m = UNLEARN.match(t)
        if m:
            n = self.unlearn(m.group(1))
            return "Forgotten." if n else "I wasn't taught anything about that."
        if re.match(r"^what (?:have you|did you|else have you) learn(?:ed|t)?(?: so far| from me)?$", t, re.I):
            lr = self.rec.get("learned", [])
            if not lr:
                return "Nothing yet. Teach me! Say when someone says this, say that."
            last = lr[-1]["q"][0]
            return f"{len(lr)} things so far. The newest: what to say to {last}." if len(lr) > 1 else f"One thing: what to say to {last}."
        m = re.match(r"^(?:forget|delete|remove) (?:the |that )?(?:place |spot )?(?:called |named )?(.+?)(?: (?:place|spot|area))?$", t, re.I)
        if m and self.place(m.group(1)):
            name = self.forget_place(m.group(1))
            return f"The {name}? Gone from my map."
        return None

    # -- what people say --
    def remark(self, who, tone: int, heard: str, t_heard: float, line: Optional[str] = None, overheard: bool = False):
        c, lib = self.cfg, self.lib
        p = self.person(who)
        self.event(self.as_who(f"you said {'nice' if tone > 0 else 'mean'} things {'about' if overheard else 'to'} her: "
                               f"\"{heard[:50]}\"", who))
        now = time.time()
        pick = lambda k: Library.pick(self.picks, k, lib.line(k))
        self.auto_feedback(who, tone > 0)
        if tone > 0:
            p.good("overheard kindness" if overheard else "kind words", self)
            self.face("happy", 4)
            self.tell_favourite()
            return self.speak(line or pick("overheard_nice" if overheard else "nice_reply"), "kind words", t_heard)
        wiped = p.bad("overheard meanness" if overheard else "mean words", self, c.get("cancel_after", 3))
        self.tell_favourite()
        to = f" to={p.name}" if p.name != "you" else ""
        if c.get("mean", True) and (wiped or (p.score() <= -2 and now - p.last_snow > 20)):
            p.last_snow = now
            self.order("throw" + to)
            self.face("angry", 8)
            return self.speak(pick("mean_back") if wiped else (line or pick("mean_reply")), "mean back (snowball)", t_heard)
        self.face("annoyed", 6)
        if not overheard and self.sorry_due(p):                        # in the red: she wants an apology
            line = f"{line} {pick('ask_sorry')}" if line else pick("ask_sorry")
            return self.speak(line, "mean words, asks for an apology", t_heard)
        return self.speak(line or pick("overheard_mean" if overheard else "mean_reply"), "mean words", t_heard)

    def sorry_due(self, p: Person) -> bool:
        """In the red and not asked for an apology lately (she asks, never naming them)."""
        if p.score() >= 0 or p.mean <= 0 or time.time() - p.asked_sorry_at < float(self.cfg.get("ask_sorry_every_s", 45)):
            return False
        p.asked_sorry_at = time.time()
        return True

    def handle(self, heard: str, t_heard: float, who: Optional[str] = None, named: bool = False):
        """What `who` said to her (see satellite_ai.py handle()). Teaching and places are the
        player's whose game runs her; everyone has their own profile. `named`: her name was said
        (it's already taken out of `heard`)."""
        c, lib = self.cfg, self.lib
        me = self.is_me(who)
        p = self.person(who)
        p.last_spoke = self.last_heard = time.time()
        self.log(f"{'you' if me else p.name}:", heard)
        self.turn("player" if me else p.name, heard)
        said = lambda text: self.event(self.as_who(text, who))
        pick = lambda k: Library.pick(self.picks, k, lib.line(k))

        bar = lib.barrier.check_in(heard)                              # racist / sexist: refused, and mean
        if bar:
            self.log(f"  barrier: {p.name} ({bar})")
            return self.remark(who, -1, heard, t_heard, pick("refuse_slur" if bar == "slur" else "refuse_bigot"))
        sorry = apology(heard, named)
        if sorry and (sorry == "real" or p.mean > 0):
            said(f"you said \"{heard[:60]}\"")
            if sorry == "bare":                                        # "sorry", "my bad", "oops": nothing
                self.face("annoyed", 3)
                self.log(f"  {p.name}: not a real apology (mean {p.mean:.1f} stays)")
                return self.speak(pick("sorry_mean_it"), "apology, not meant", t_heard)
            if p.mean <= 0:
                return self.speak(pick("sorry_nothing"), "apology, nothing to forgive", t_heard)
            p.forgive()
            p.asked_sorry_at = 0.0
            self.face("happy", 4)
            self.log(f"  {p.name} apologised: mean points reset, score {p.score():+.1f}")
            self.tell_favourite()
            return self.speak(pick("sorry_accepted"), "apology accepted", t_heard)

        def order(text: str, how: str, do, line: str, undo=None):
            # stopping is a brake, never refused: however mean someone was, "stop" stops her (user)
            no, grumble = (None, None) if text in ("stop", "stay") else self.refusal(who, text, heard)
            if no:
                if undo:
                    undo()
                said(f"you told her to {text} (she refused)")
                self.face("annoyed", 4)
                return self.speak(no, f"refused {how}", t_heard)
            self.obey(who)
            do()
            said(f"you told her to {text}")
            if grumble:
                self.face("annoyed", 4)
            return self.speak(grumble or line, how + (" (grumbling)" if grumble else ""), t_heard)

        if me:
            t = self.teach_instant(heard)
            if t:
                return self.speak(t, "taught", t_heard)
        tg = lib.chat(self.picks, heard, 0.68, 0.0, self.learned())
        if tg:
            said(f"you said \"{heard[:60]}\"")
            return self.speak(tg[2], f"learned {tg[1]} {tg[0]:.2f}", t_heard)
        m = re.search(r"\b(?:remember|save|mark|call)\b.*?\b(?:this|here|spot|place)\b.*?" + SPOT_NAMED + r"\s+(?:the\s+)?(.+)$",
                      heard, re.I) if me else None
        g = re.search(r"\b(?:go|fly|head)\s+(?:back\s+)?to\s+(?:the\s+)?(.+)$", heard, re.I)
        bare = re.sub(r"\b(now|please|go|the|to|back)\b", "", heard.lower()).strip(" .,!?")
        if not m and not g and len(bare) >= 4 and len(bare.split()) <= 3 and self.place(bare):   # just a place
            g = re.match(r"(.+)", bare)
        if g and not m and not self.place(g.group(1)) and not instant(heard) and \
                not re.search(r"\b(spawn|home|start|middle|cent(er|re)|me|you|sleep|bed|hell)\b", g.group(1), re.I):
            want = g.group(1).strip(" .,!?")
            said(f"you asked her to go to {want} (she didn't know it)")
            return self.speak(f"I don't know where {want} is. Stand there and say, remember this spot as {want}.",
                              "unknown place", t_heard)
        if m or (g and self.place(g.group(1))):
            if m:
                name = m.group(1).strip(" .!?,").lower()
                ok = bool(self.carry_out(["mark"], name))
                return self.speak(f"Got it, the {name}!" if ok else "I can't see where you are right now.", "place", t_heard)
            hit = self.place(g.group(1))
            return order(f"go to the {hit[0]}", "place", lambda: self.carry_out(["goto"], g.group(1)), f"Off to the {hit[0]}!")
        st = lib.smalltalk(self.picks, heard)
        tone = {"compliment": 1, "love": 1, "insult": -1}.get(st[0], 0) if st else 0
        if not tone and not self._peek_move(heard) and not instant(heard):
            tone = lib.sentiment(heard, addressed=True)
        if tone:
            return self.remark(who, tone, heard, t_heard, st[1] if st and st[0] in ("compliment", "love", "insult") else None)
        pick = lambda k: Library.pick(self.picks, k, lib.line(k))
        t = self.memory_instant(who, heard)
        if t:
            said(f"you said \"{heard[:60]}\"")
            return self.speak(t, "memory", t_heard)
        if re.search(r"\b(i never said|i didn'?t say|i did not say|that'?s not what i said|you misheard)\b", heard, re.I):
            said(f"you said \"{heard[:60]}\"")
            return self.speak(pick("misheard"), "misheard", t_heard)
        if re.search(r"\b(change (your |ur )?face|make a face|(new|different|another) face|pull a face)\b", heard, re.I):
            mood = random.choice(["angry", "dizzy", "surprised", "hurt", "annoyed"])
            self.face(mood, 5)
            return self.speak(pick("new_face"), f"face {mood}", t_heard)
        was = (self.spinning, self.last_move)
        mv = self.move_instant(heard)
        if mv:                                                         # (refused: she isn't spinning after all)
            def undo():
                self.spinning, self.last_move = was
            return order(mv[0].replace(" keep", " and keep going"), f"instant {mv[0]}",
                         lambda: self.order(self.aim(mv[0], who)), mv[1], undo)
        fast = instant(heard)
        if fast:
            return order(fast, f"instant {fast}", lambda: self.order(self.aim(fast, who)),
                         Library.pick(self.picks, "order/" + fast, lib.line("order/" + fast)))
        if st:
            if st[0] == "be_quiet" and me:
                self.quiet_until = time.time() + 120
            said(f"you said \"{heard[:60]}\"")
            return self.speak(st[1], f"small talk {st[0]}", t_heard)
        ch = lib.chat(self.picks, heard)
        ans = lib.answer(self.picks, heard) if is_question(heard) and not (ch and ch[0] >= 0.85) else None
        if ans:
            said(f"you asked about {ans[1].replace('_', ' ')}")
            return self.speak(ans[2], f"knowledge {ans[1]} {ans[0]:.0f}", t_heard)
        if ch:
            score, cid, line, o = ch
            if o:
                return order(o, f"chat {cid} {score:.2f}", lambda: self.carry_out([o], who=who), line)
            said(f"you said \"{heard[:60]}\"")
            return self.speak(line, f"chat {cid} {score:.2f}", t_heard)
        au = lib.chat(self.picks, heard, 0.8, 0.0, auto_pool(lib))      # what she learned by herself
        if au:
            score, aid, line, o = au
            e = auto_find(aid)
            if e and lib.barrier.screen(line):                         # (learned before the barrier)
                self_rec()["auto"].remove(e)
                auto_changed()
                self.log(f"  barrier: dropped {aid}: {line[:60]!r}")
                e, line, o = None, pick("screened"), ""
            if e:
                e["uses"] = e.get("uses", 0) + 1
                store().mark(SELF)
            self.last_auto = (aid, time.time(), who)
            said(f"you said \"{heard[:60]}\"")
            if o:
                return order(o, f"self-learned {aid} {score:.2f}", lambda: self.carry_out([o], who=who), line)
            return self.speak(line, f"self-learned {aid} {score:.2f}", t_heard)
        if is_question(heard):                                         # something they told her to remember?
            hit = self.recall(who, heard, need=2)
            if hit:
                said(f"you asked about \"{heard[:50]}\"")
                return self.speak(f"You told me: {hit}.", "recalled", t_heard)
        if len(re.findall(r"[a-z']+", heard.lower())) <= 2:            # "now", "oh", "well": no "don't know"
            return self.speak(pick("filler"), "filler", t_heard)
        self.unknown(heard)
        if me:
            self.last_unknown, self.last_unknown_at = heard, time.time()
        if self.think(heard, who, t_heard):                            # her chat model answers (in a moment)
            said(f"you said \"{heard[:60]}\"")
            return
        if not is_question(heard) and len(heard.split()) > 5:          # talk, not a question: nod (and log it)
            return self.speak(pick("nod"), "nod", t_heard)
        said(f"you said \"{heard[:60]}\" (she didn't know)")
        return self.speak(c["unknown"], "unknown", t_heard)

    # -- her own chat model (edith_mind.py) --
    def think(self, heard: str, who: Optional[str], t_heard: float) -> bool:
        """Ask her chat model, off the event loop; she answers when it's done. False: no model."""
        if CHAT.tried and CHAT.llm is None:
            return False
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return False
        speaker = who if who and who != "you" else (self.me or "you")
        pr = self.prof(who)
        about = [f"call them {pr['nickname']}"] if pr.get("nickname") else []
        about += [f"likes {x}" for x in pr["likes"][-3:]] + [f"doesn't like {x}" for x in pr["dislikes"][-2:]]
        about += [f"favorite {k} is {v}" for k, v in list(pr["favorites"].items())[-2:]] + pr["notes"][-2:]
        facts = self.rec["facts"] if self.is_me(who) else []
        turns = []
        for w, x in self.rec["turns"][:-1][-6:]:                        # (the last one is this line)
            turns.append(("assistant" if w == "her" else "user", x))
        system = system_prompt(speaker, about, [m for m in self.members if m != speaker], facts)
        self.log("  thinking (chat model)...")
        loop.create_task(self._think(system, turns, heard, who, t_heard))
        return True

    async def _think(self, system: str, turns: list, heard: str, who: Optional[str], t_heard: float):
        pick = lambda k: Library.pick(self.picks, k, self.lib.line(k))
        try:
            text = await asyncio.wait_for(asyncio.to_thread(CHAT.reply, system, turns, heard), 15.0)
        except Exception as e:  # noqa: BLE001
            self.log(f"  chat model failed: {type(e).__name__} {e}"[:200])
            text = None
        if self.sess.closed:
            return
        line, act = tidy(text) if text else ("", None)
        line = self.unname(line)
        blocked = self.lib.barrier.screen(line) if line else None
        if blocked:                                                    # never said, never kept
            self.log(f"  barrier: chat model reply blocked ({blocked}): {line[:80]!r}")
            line, act = Library.pick(self.picks, "screened", self.lib.line("screened")), None
        if not line:
            line = pick("nod") if not is_question(heard) and len(heard.split()) > 5 else self.cfg["unknown"]
            self.speak(line, "chat model gave nothing", t_heard)
            self.flush()
            return
        refused = False
        if act:
            no, grumble = (None, None) if act == "stay" else self.refusal(who, act, heard)
            if no:
                line, act, refused = no, None, True
            else:
                self.obey(who)
                self.carry_out([act], who=who)
                refused = bool(grumble)                                # (a grumble is about this player)
                line = grumble or line
        if not refused and not blocked and not no_learn(line, who, self.me, self.members):
            self.learn_auto(heard, line, act, who)
        self.speak(line, f"chat model {CHAT.last_ms} ms{' + ' + act if act else ''}", t_heard)
        self.flush()

    def unname(self, line: str) -> str:
        """Her chat model's line without anyone's name in it (she never says names, user 2026-10-18)."""
        names = {n for n in list(self.members) + list(self.positions) + [self.me] if n and n != "you" and len(n) >= 3}
        for n in sorted(names, key=len, reverse=True):
            for form in {n, n.replace("_", " ")}:
                line = re.sub(rf"\s*,?\s*\b{re.escape(form)}\b", "", line, flags=re.I)   # "Hey Ann, do" -> "Hey, do"
        line = re.sub(r",\s*([.!?])", r"\1", re.sub(r"\s+([.,!?])", r"\1", re.sub(r"\s+", " ", line))).strip(" ,")
        return line[:1].upper() + line[1:]

    def learn_auto(self, heard: str, line: str, act: Optional[str], who: Optional[str]):
        """Her chat model's answer, kept: the same question next time is instant (and so is its voice)."""
        if len(heard.split()) > 14 or len(heard) < 6:                  # (the chat matcher skips long lines)
            return
        rec = self_rec()
        lr = rec["auto"]
        _auto.setdefault("n", max([int(e["id"].split("_")[-1]) for e in lr] or [0]))
        _auto["n"] += 1
        e = {"id": f"auto_{_auto['n']}", "q": [heard.strip(" .!?,")], "a": [line], "do": act or "",
             "at": time.strftime("%Y-%m-%d %H:%M", time.gmtime()), "by": who or self.me or "?", "uses": 0, "good": 0}
        lr.append(e)
        if len(lr) > AUTO_MAX:                                         # the least used go first
            lr.sort(key=lambda x: (x.get("uses", 0) + 2 * x.get("good", 0), x["at"]), reverse=True)
            del lr[AUTO_MAX:]
        self.last_auto = (e["id"], time.time(), who)
        auto_changed()
        self.log(f"  learned by herself: {e['id']} {heard[:50]!r} -> {line!r}")

    def auto_feedback(self, who: Optional[str], good: bool):
        """Thanks / told off right after one of her self-learned answers: it gains, or it's dropped."""
        if not self.last_auto or time.time() - self.last_auto[1] > 20 or self.last_auto[2] != who:
            return
        e = auto_find(self.last_auto[0])
        self.last_auto = None
        if not e:
            return
        if good:
            e["good"] = e.get("good", 0) + 1
            store().mark(SELF)
        else:
            self_rec()["auto"].remove(e)
            auto_changed()
            self.log(f"  dropped {e['id']} (told off): {e['q'][0][:40]!r} -> {e['a'][0]!r}")

    # -- her own initiative (user, 2026-10-19) --
    def initiative(self):
        """On each state line: when nobody has talked to her for idle_s, she picks someone and goes
        to them (an apology first from anyone in the red; else someone she likes, someone quiet, or
        the middle of the crowd) and opens small talk; or she roams. Orders (EDITH.dll):
            approach <name> | follow crowd | roam      when the state line has players= (positions)
            come <name>                                 the fallback without it (older EDITH.dll)
        Only the host's (or a solo) Edith decides; a player's order holds her for after_order_s."""
        ini = self.lib.ini()
        now = time.time()
        if not ini.get("on", True) or self.role == "puppet" or not self.me or not self.in_lobby():
            return
        if self.busy() or now < self.quiet_until or now - self.last_heard < float(ini["idle_s"]) \
                or now - self.last_initiative < float(ini["every_s"]):
            return
        if self.order_by and now - self.order_at < float(ini["after_order_s"]):
            return
        self.last_initiative = now
        pos = self.positions
        people = [n for n in (pos or self.members or [self.me]) if n and n != "you" and self.modded(n)]
        if not people:
            return
        listen = float(ini["listen_s"])

        def go(name: str, mode: str):
            self.order(f"approach {name}" if pos else f"come {name}")
            if not pos:
                self.order(f"lookat 8 {name}")
            self.opened = (name, now)
            self.person(name).awake_until = now + listen                # they can answer without her name
            self.log(f"initiative: {mode} -> {name} ({self.score(name):+.1f})")

        red = [n for n in people if self.score(n) < 0 and self.person(n).mean > 0
               and now - self.person(n).asked_sorry_at >= float(ini["sorry_every_s"])]
        if red:                                                        # in the red: an apology, please
            name = min(red, key=self.score)
            self.person(name).asked_sorry_at = now
            go(name, "wants an apology")
            self.face("annoyed", 4)
            return self.speak(Library.pick(self.picks, "ask_sorry", self.lib.line("ask_sorry")), "initiative: apology")
        friends = [n for n in people if self.score(n) >= 0]
        if pos and (not friends or random.random() < float(ini["roam_chance"])):
            self.order("roam")
            self.log("initiative: roam")
            return
        if not friends:
            return
        w = dict(ini.get("weights") or {})
        liked = [n for n in friends if self.score(n) > 0]
        modes = {"liked": bool(liked), "quiet": True, "crowd": len(pos) >= 3}
        choices = [m for m in modes if modes[m] and float(w.get(m, 0)) > 0] or ["quiet"]
        mode = random.choices(choices, [float(w.get(m, 1)) or 1.0 for m in choices])[0]
        if mode == "liked":
            top = max(self.score(n) for n in liked)
            name = random.choice([n for n in liked if self.score(n) >= top - 0.5])
        elif mode == "crowd":                                          # the middle of everyone (mod or not)
            mid = [sum(v[i] for v in pos.values()) / len(pos) for i in range(3)]
            name = min((n for n in friends if n in pos), key=lambda n: math.dist(pos[n], mid), default=friends[0])
        else:                                                          # whoever talked to her least lately
            oldest = min(self.person(n).last_spoke for n in friends)
            name = random.choice([n for n in friends if self.person(n).last_spoke <= oldest + 30])
        if mode == "crowd":
            self.order("follow crowd")
            self.opened = (name, now)
            self.person(name).awake_until = now + listen
            self.log(f"initiative: crowd (talks to {name}, nearest the middle)")
        else:
            go(name, mode)
        self.open_talk(name, mode)

    def open_talk(self, who: str, why: str):
        """Small talk she starts: her chat model with what she knows about them, else a stock opener."""
        pr = self.prof(who)
        likes = pr.get("likes") or []
        if likes:
            stock = Library.pick(self.picks, "opener_like", self.lib.line("opener_like")).replace("{thing}", random.choice(likes[-3:]))
        else:
            stock = Library.pick(self.picks, "opener", self.lib.line("opener"))
        if CHAT.tried and CHAT.llm is None:
            return self.speak(stock, f"initiative: {why}, small talk")
        about = [f"likes {x}" for x in likes[-3:]] + [f"doesn't like {x}" for x in pr["dislikes"][-2:]]
        about += [f"favorite {k} is {v}" for k, v in list(pr["favorites"].items())[-2:]] + pr["notes"][-2:]
        system = opener_prompt(who, about, [m for m in self.members if m != who])
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return self.speak(stock, f"initiative: {why}, small talk")
        self.log("  thinking of something to say (chat model)...")
        loop.create_task(self._open(system, who, why, stock, time.time()))

    async def _open(self, system: str, who: str, why: str, stock: str, t0: float):
        try:
            text = await asyncio.wait_for(asyncio.to_thread(CHAT.reply, system, [], "(You start the conversation.)"), 15.0)
        except Exception as e:  # noqa: BLE001
            self.log(f"  chat model failed: {type(e).__name__} {e}"[:200])
            text = None
        if self.sess.closed or self.last_heard > t0 or self.busy():      # someone talked to her meanwhile
            return
        line = self.unname(tidy(text)[0]) if text else ""
        if not line or self.lib.barrier.screen(line):
            line = stock
        self.speak(line, f"initiative: {why}, small talk ({CHAT.last_ms} ms)")
        self.flush()

    # -- reports for her owner (edith_mind.py) --
    def check_report(self, text: str, who: Optional[str]):
        speaker = who if who and who != "you" else self.me
        if not speaker or speaker == OWNER:
            return
        hit = detect(text)
        if not hit:
            return
        kind, said = hit
        now = time.time()
        key = (speaker, kind, said.lower() if kind == "message" else "")  # (every Edith in the match hears it:
        if now - _report_seen.get(key, 0) < 120:                       #  one report; each message once)
            return
        _report_seen[key] = now
        r = {"kind": kind, "who": speaker, "said": said[:300], "at": time.strftime("%Y-%m-%d %H:%M", time.gmtime()),
             "by": self.me or "?", "told": False}
        rec = self_rec()
        rec["reports"].append(r)
        del rec["reports"][:-REPORTS_MAX]
        store().mark(SELF)
        self.log(f"  report ({kind}): {speaker}: {text[:80]!r}")
        try:
            asyncio.get_running_loop().create_task(post_webhook(r))
        except RuntimeError:
            pass

    def tell_reports(self):
        """Her owner just spawned in her lobby: what she kept for them while they were away."""
        if self.me != OWNER:
            return
        pending = [r for r in self_rec()["reports"] if not r.get("told")]
        if not pending:
            return
        line = tell_line(pending[-10:])
        for r in pending:
            r["told"] = True
        store().mark(SELF)
        if line:
            self.speak(line, f"reports for {OWNER} ({len(pending)})")

    @staticmethod
    def unknown(heard: str):
        """What to add to her library next (GET /edith/brain/unknown)."""
        try:
            with open(DATA / "unknown.txt", "a", encoding="utf-8") as f:
                f.write(heard + "\n")
        except OSError:
            pass

    def _peek_move(self, heard: str) -> bool:
        """Is this a move order? (move_instant without its side effects: "spin please" isn't a remark)"""
        saved = (self.spinning, self.last_move, self.last_push, dict(self.picks))
        try:
            return bool(self.move_instant(heard))
        finally:
            self.spinning, self.last_move, self.last_push, self.picks = saved

    # -- an utterance, heard (satellite_ai.py main loop) --
    def modded(self, who: Optional[str]) -> bool:
        """Does this player run E.D.I.T.H? (her room members: the relay groups only players with the
        mod). She ignores everyone else (user, 2026-10-18): they can't even see her."""
        return not who or who in ("you", self.me) or who in self.members

    def wants_audio(self, who: Optional[str]) -> bool:
        """Worth transcribing? Only in her lobby; your mic not while muted or while she talks; a
        voice chat voice not on a puppet, nor one whose own companion hears them, nor anyone without
        the mod."""
        if not self.in_lobby():
            return False
        if not who:
            return not (self.muted and time.time() - self.muted_at < 6)
        if not self.modded(who):
            return False
        return not (self.role == "puppet" or who in self.with_ai or time.time() - self.forwarded.get(who, 0) < 30)

    def on_heard(self, text: str, who: Optional[str], t_heard: float):
        c = self.cfg
        mine = not who
        text = (text or "").strip()
        if not text or len(text) < 2:
            return
        if not self.in_lobby():
            return
        if self.modded(who) and not (mine and self.role == "puppet"):
            self.check_report(text, who)
        p = self.person(who)
        cmd = strip_wake(c, text)
        named = cmd is not None
        if cmd is None and time.time() < p.awake_until and len(text.split()) <= 10:
            cmd = text
        if cmd is None and self.spinning and len(text.split()) <= 4 and re.search(r"\b(stop|enough|halt)\b", text, re.I):
            cmd = text                                                 # an endless spin: a bare "stop" is for her
        if cmd is None:
            tone = self.lib.sentiment(text, addressed=False)
            if tone and not (mine and self.role == "puppet") and time.time() - p.last_spoke > 5:
                self.log(f"(about her{'' if mine else ', ' + p.name})", text)
                p.last_spoke = time.time()
                self.remark(who, tone, text, t_heard, overheard=True)
            else:
                self.log(f"(not for her{'' if mine else ', ' + p.name})", text)
            self.flush()
            return
        p.awake_until = time.time() + c["follow_up_seconds"] if named else 0.0
        if mine and self.role == "puppet":                             # the host's Edith answers
            self.order(f"heard {text}")
            self.log(f"  -> host ({self.state.split('host=')[-1].split()[0] if 'host=' in self.state else '?'}): {text}")
            self.flush()
            return
        self.log(f"  heard{'' if mine else ' ' + p.name} in {time.time() - t_heard:.2f} s")
        speaker = p.name if who else self.me                   # she turns to whoever talks to her, whatever
        if speaker and speaker != "you":                       # their score (user, 2026-10-18)
            self.order(f"lookat 6 {speaker}")
        self.handle(cmd, t_heard, who, named)
        self.flush()


# ---- ears (Whisper) and mouth (Kokoro) ------------------------------------------------------------

class Ears:
    """faster-whisper base.en, int8 on the CPU. Loaded in the background at import."""

    def __init__(self):
        self.model = None
        self.error = ""
        self.lock = threading.Lock()
        self.ready = threading.Event()

    def load(self):
        try:
            from faster_whisper import WhisperModel
            root = os.environ.get("EDITH_WHISPER_DIR") or str(DATA / "whisper")
            self.model = WhisperModel(os.environ.get("EDITH_WHISPER", "base.en"), device="cpu", compute_type="int8",
                                      cpu_threads=int(os.environ.get("EDITH_WHISPER_THREADS", "4")), download_root=root)
            log("whisper ready")
        except Exception as e:  # noqa: BLE001
            self.error = f"{type(e).__name__}: {e}"[:200]
            log("whisper unavailable:", self.error)
        self.ready.set()

    def transcribe(self, pcm: bytes, name: str) -> str:
        import numpy as np
        self.ready.wait(300)
        if self.model is None:
            return ""
        x = np.frombuffer(pcm, dtype=np.int16).astype(np.float32) / 32768.0
        with self.lock:
            segs, _ = self.model.transcribe(x, language="en", beam_size=1, vad_filter=False,
                                            hotwords=name, initial_prompt=prime(name))
            return " ".join(s.text for s in segs).strip()


class Mouth:
    """Kokoro (82M int8 ONNX, CPU), 24 kHz mono: lines the voice pack lacks. Renders are cached on
    disk by their pack key (the fallback voice's)."""

    RATE = 24000
    URL = "https://github.com/thewh1teagle/kokoro-onnx/releases/download/model-files-v1.0/"

    def __init__(self):
        self.k = None
        self.error = ""
        self.lock = threading.Lock()
        self.tried = False
        self.cache = DATA / "voice"

    def _load(self):
        self.tried = True
        try:
            from kokoro_onnx import Kokoro
            import onnxruntime as ort
            d = Path(os.environ.get("EDITH_KOKORO_DIR") or (DATA / "kokoro"))
            d.mkdir(parents=True, exist_ok=True)
            for f in ("kokoro-v1.0.int8.onnx", "voices-v1.0.bin"):
                if not (d / f).exists():
                    import urllib.request
                    log("kokoro: downloading", f)
                    urllib.request.urlretrieve(self.URL + f, str(d / f) + ".part")
                    os.replace(str(d / f) + ".part", d / f)
            so = ort.SessionOptions()
            so.intra_op_num_threads = int(os.environ.get("EDITH_VOICE_THREADS", "2"))
            so.inter_op_num_threads = 1
            sess = ort.InferenceSession(str(d / "kokoro-v1.0.int8.onnx"), so, providers=["CPUExecutionProvider"])
            self.k = Kokoro.from_session(sess, str(d / "voices-v1.0.bin"))
            self.cache.mkdir(parents=True, exist_ok=True)
            log("kokoro ready")
        except Exception as e:  # noqa: BLE001
            self.error = f"{type(e).__name__}: {e}"[:200]
            log("kokoro unavailable:", self.error)

    def render(self, text: str) -> bytes:
        import numpy as np
        cfg = LIB.get().cfg
        voice, speed = cfg["fallback_voice"], cfg["voice_speed"]
        f = self.cache / f"{voice_key(voice, speed, text)}.pcm"
        if f.exists():
            return f.read_bytes()
        with self.lock:
            if not self.tried:
                self._load()
            if self.k is None:
                raise RuntimeError(self.error or "no voice")
            a, sr = self.k.create(text, voice=voice, speed=float(speed), lang="en-us")
        if sr != self.RATE:
            x = np.linspace(0, len(a) - 1, int(len(a) * self.RATE / sr))
            a = np.interp(x, np.arange(len(a)), a)
        pcm = (np.clip(a, -1, 1) * 32767).astype(np.int16).tobytes()
        try:
            f.write_bytes(pcm)
        except OSError:
            pass
        return pcm


EARS = Ears()
MOUTH = Mouth()


def _start():
    DATA.mkdir(parents=True, exist_ok=True)
    try:
        seed_library()
    except Exception as e:  # noqa: BLE001
        log("library: seeding failed:", e)
    LIB.get()
    if os.environ.get("EDITH_STT", "1") != "0":
        threading.Thread(target=EARS.load, daemon=True).start()
    else:
        EARS.ready.set()
    if os.environ.get("EDITH_TTS", "1") != "0":
        def warm_mouth():
            with MOUTH.lock:
                if not MOUTH.tried:
                    MOUTH._load()
        threading.Thread(target=warm_mouth, daemon=True).start()
    if os.environ.get("EDITH_CHAT", "1") != "0":
        def warm_chat():
            with CHAT.lock:
                if not CHAT.tried:
                    CHAT.load()
        threading.Thread(target=warm_chat, daemon=True).start()


_start()


# ---- the WebSocket ----------------------------------------------------------------------------------

class Session:
    def __init__(self, ws: WebSocket):
        self.ws = ws
        self.hello_name = ""
        self.out: asyncio.Queue = asyncio.Queue(maxsize=4096)
        # her ears: your own mic first, other players' voice chat after it (a lobby full of chatter
        # filled one shared queue and dropped / delayed "Edith, come here" by 10 s, user 2026-10-18)
        self.audio: asyncio.Queue = asyncio.Queue(maxsize=4)            # your mic
        self.audio_chat: asyncio.Queue = asyncio.Queue(maxsize=3)       # voice chat: the oldest goes when full
        self.audio_ready = asyncio.Event()
        self.renders: asyncio.Queue = asyncio.Queue(maxsize=16)
        self.windows: Dict[str, tuple] = {}
        self.closed = False
        self.ed = Edith(self)

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

    def send(self, msg):
        if self.closed:
            return
        data = msg if isinstance(msg, (bytes, str)) else json.dumps(msg, separators=(",", ":"))
        try:
            self.out.put_nowait(data)
        except asyncio.QueueFull:
            pass


_sessions: set = set()
_flusher: Optional[asyncio.Task] = None


async def _flush_loop():
    while True:
        await asyncio.sleep(2.0)
        items = store().take()
        if items:
            await asyncio.to_thread(store().write, items)


async def _sender(s: Session):
    try:
        while not s.closed:
            data = await s.out.get()
            if isinstance(data, bytes):
                await s.ws.send_bytes(data)
            else:
                await s.ws.send_text(data)
    except Exception:  # noqa: BLE001
        s.closed = True


#: Whisper runs on the shared CPU: a couple at a time, so one busy lobby can't slow every session
WHISPER_SLOTS = asyncio.Semaphore(2)
#: voice-chat clips longer than this are chatter, not a word to her ("Edith, come here" is ~1.5 s);
#: transcribing them took 5-9 s each and starved everyone's queue
MAX_CHAT_SECONDS = 7.0


async def _ears(s: Session):
    while not s.closed:
        if s.audio.empty() and s.audio_chat.empty():
            s.audio_ready.clear()
            await s.audio_ready.wait()
            continue
        q = s.audio if not s.audio.empty() else s.audio_chat          # your own voice first
        who, pcm, t = q.get_nowait()
        if not s.ed.wants_audio(who):
            continue
        name = s.ed.cfg["name"]
        t0 = time.time()
        try:
            async with WHISPER_SLOTS:
                text = await asyncio.to_thread(EARS.transcribe, pcm, name)
        except Exception as e:  # noqa: BLE001
            log("whisper failed:", e)
            continue
        if text:
            s.ed.log(f"heard{' ' + who if who else ''} ({len(pcm) / 32000:.1f} s, whisper {time.time() - t0:.2f} s): {text}")
        s.ed.on_heard(text, who, t)


async def _mouth(s: Session):
    while not s.closed:
        rid, text = await s.renders.get()
        try:
            for part in sentences(text) or [text]:
                pcm = await asyncio.to_thread(MOUTH.render, part)
                for i in range(0, len(pcm), 9600):                     # 0.2 s per frame
                    s.send(b"EDV1" + struct.pack("<I", rid) + pcm[i:i + 9600])
            s.send({"t": "rendered", "id": rid, "ok": True})
        except Exception as e:  # noqa: BLE001
            s.ed.log("voice render failed:", e)
            s.send({"t": "rendered", "id": rid, "ok": False})


def _text(s: Session, msg: dict):
    t = msg.get("t")
    ed = s.ed
    if t == "hello":
        s.hello_name = str(msg.get("name") or "")[:48]
        try:
            ed.tz_min = max(-14 * 60, min(14 * 60, int(msg.get("tz") or 0)))
        except (TypeError, ValueError):
            pass
        c = ed.cfg
        s.send({"t": "hi", "name": c["name"], "voice": c["voice"], "fallback": c["fallback_voice"],
                "speed": c["voice_speed"], "stt": EARS.model is not None or not EARS.ready.is_set(),
                "tts": MOUTH.k is not None or not MOUTH.tried})
        log(f"companion connected: {s.hello_name or '(no name yet)'} (build {str(msg.get('build'))[:32]})")
    elif t == "dll":
        line = str(msg.get("line") or "")
        if line:
            ed.on_dll(line)
    elif t == "text":
        text = str(msg.get("text") or "").strip()
        if text:
            cmd = strip_wake(ed.cfg, text)
            ed.handle(cmd or text, time.time(), msg.get("who") or None, cmd is not None)
            ed.flush()
    elif t == "speaking":
        ed.speaking_until = time.time() + 30.0 if msg.get("on") else 0.0
    elif t == "render":
        text = say_clean(str(msg.get("text") or ""))[:600]
        if text and s.rate_ok("render", MAX_RENDERS_PER_MIN, 60.0):
            try:
                s.renders.put_nowait((int(msg.get("id") or 0) & 0xFFFFFFFF, text))
            except asyncio.QueueFull:
                s.send({"t": "rendered", "id": msg.get("id"), "ok": False})
        else:
            s.send({"t": "rendered", "id": msg.get("id"), "ok": False})


def _binary(s: Session, data: bytes):
    if data[:4] != b"EDA1" or len(data) < 6 or len(data) > MAX_AUDIO + 64:
        return
    n = data[4]
    who = data[5:5 + n].decode("utf-8", "replace") or None
    pcm = data[5 + n:]
    if len(pcm) < 16000 * 2 * 0.25 or not s.rate_ok("audio", MAX_AUDIO_PER_MIN, 60.0):
        return
    if not s.ed.wants_audio(who):
        return
    item = (who, pcm[: len(pcm) // 2 * 2], time.time())
    if who:                                                    # voice chat: short clips only, newest kept
        if len(pcm) / 32000 > MAX_CHAT_SECONDS:
            return
        if s.audio_chat.full():
            s.audio_chat.get_nowait()
        s.audio_chat.put_nowait(item)
    else:
        try:
            s.audio.put_nowait(item)
        except asyncio.QueueFull:
            log("audio queue full: one of your own utterances dropped")
            return
    s.audio_ready.set()


@router.websocket("/edith/brain")
async def brain_ws(ws: WebSocket):
    global _flusher
    if len(_sessions) >= MAX_SESSIONS:
        await ws.close(code=1013)
        return
    await ws.accept()
    if _flusher is None or _flusher.done():
        _flusher = asyncio.create_task(_flush_loop())
    s = Session(ws)
    _sessions.add(s)
    tasks = [asyncio.create_task(f(s)) for f in (_sender, _ears, _mouth)]
    try:
        while not s.closed:
            msg = await ws.receive()
            if msg.get("type") == "websocket.disconnect":
                break
            text = msg.get("text")
            if text is not None:
                if len(text) > MAX_TEXT or not s.rate_ok("text", 60, 1.0):
                    continue
                try:
                    data = json.loads(text)
                except ValueError:
                    continue
                if isinstance(data, dict):
                    try:
                        _text(s, data)
                    except Exception as e:  # noqa: BLE001 -- one bad line never drops her
                        log("brain error:", type(e).__name__, e)
                continue
            data = msg.get("bytes")
            if data is not None:
                _binary(s, data)
    except WebSocketDisconnect:
        pass
    except Exception as e:  # noqa: BLE001
        log("session error:", e)
    finally:
        s.closed = True
        _sessions.discard(s)
        for t in tasks:
            t.cancel()
        log(f"companion gone: {s.ed.me or s.hello_name or '?'}")


# ---- HTTP: stats, and her library edited live (ADMIN_TOKEN) ----------------------------------------

def _admin(request: Request, token: Optional[str]):
    if not ADMIN_TOKEN:
        raise HTTPException(status_code=503, detail="ADMIN_TOKEN is not configured")
    supplied = token or request.headers.get("x-admin-token")
    if not supplied or not hmac.compare_digest(supplied, ADMIN_TOKEN):
        raise HTTPException(status_code=404, detail="Not found")


@router.get("/edith/brain/stats")
async def brain_stats():
    lib = LIB.get()
    return {"companions": len(_sessions), "whisper": EARS.model is not None, "kokoro": MOUTH.k is not None,
            "chat": CHAT.llm is not None, "chat_error": CHAT.error, "chat_last_ms": CHAT.last_ms,
            "library": {"knowledge": len(lib.entries), "smalltalk": len(lib.intents), "chat": len(lib.chats)},
            "library_error": LIB.error, "store": "postgres" if store().db else "files"}


@router.get("/edith/brain/library/{name}")
async def library_get(name: str, request: Request, token: Optional[str] = None):
    _admin(request, token)
    if name not in LIB_FILES:
        raise HTTPException(status_code=404, detail="no such file")
    return json.loads((LIB_DIR / f"{name}.json").read_text(encoding="utf-8"))


@router.put("/edith/brain/library/{name}")
async def library_put(name: str, request: Request, token: Optional[str] = None):
    """Replace one library file. It must parse and build a working library first; she picks it up
    within 2 s."""
    _admin(request, token)
    if name not in LIB_FILES:
        raise HTTPException(status_code=404, detail="no such file")
    try:
        data = json.loads(await request.body())
        Library(LibraryHolder.read(LIB_DIR, {name: data}))
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=f"not applied: {type(e).__name__}: {e}"[:300])
    tmp = LIB_DIR / f"{name}.json.tmp"
    tmp.write_text(json.dumps(data, indent=1, ensure_ascii=False), encoding="utf-8")
    tmp.replace(LIB_DIR / f"{name}.json")
    LIB.checked = 0.0
    LIB.get()
    return {"ok": True, "error": LIB.error}


def _write_lib(name: str, data: dict):
    tmp = LIB_DIR / f"{name}.json.tmp"
    tmp.write_text(json.dumps(data, indent=1, ensure_ascii=False), encoding="utf-8")
    tmp.replace(LIB_DIR / f"{name}.json")
    LIB.checked = 0.0
    LIB.get()


def _stock_lines(lib: Library) -> Dict[str, str]:
    """Every stock line as the game hears it (names out, her name in) -> keyed by its pack keys."""
    c = lib.cfg
    out: Dict[str, str] = {}
    for raw in lib.all_lines():
        t = say_clean(drop_name(raw).replace("{me}", c["name"]))
        for part in [t] + (sentences(t) if len(sentences(t)) > 1 else []):
            out.setdefault(voice_key(c["voice"], c["voice_speed"], part), part)
            out.setdefault(voice_key(c["fallback_voice"], c["voice_speed"], part), part)
    return out


def _rendered(text: str) -> bool:
    c = LIB.get().cfg
    return all((MOUTH.cache / f"{voice_key(c['fallback_voice'], c['voice_speed'], p)}.pcm").exists()
               for p in sentences(text) or [text])


_prerender = {"running": False, "done": 0, "failed": [], "at": ""}


def _prerender_run(texts: list):
    """Kokoro renders of re-voiced lines, cached (per sentence, as the game asks for them)."""
    _prerender.update(running=True, done=0, failed=[], at=time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()))
    try:
        for text in texts:
            try:
                for part in sentences(text) or [text]:
                    MOUTH.render(part)
                _prerender["done"] += 1
            except Exception as e:  # noqa: BLE001
                _prerender["failed"].append(f"{text[:60]}: {type(e).__name__}: {e}"[:200])
    finally:
        _prerender["running"] = False
        log(f"revoice: pre-rendered {_prerender['done']}/{len(texts)}")


def _prerender_start(texts: list) -> bool:
    if _prerender["running"] or not texts:
        return False
    threading.Thread(target=_prerender_run, args=(list(texts),), daemon=True).start()
    return True


@router.get("/edith/brain/revoice")
async def revoice_get(request: Request, token: Optional[str] = None):
    """The re-voiced lines (bank copy broken: the game streams the server's render), each with
    whether its render is cached."""
    _admin(request, token)
    lib = LIB.get()
    return {"stream_all": lib.stream_all, "prerender": _prerender,
            "lines": [dict(e, rendered=_rendered(e["text"]) if e.get("text") else False) for e in lib.revoice]}


@router.post("/edith/brain/revoice")
async def revoice_add(request: Request, token: Optional[str] = None):
    """Mark a line broken: {"text": "..."} or {"key": "<pack key or alt>"} (a stock line's), optional
    "why"; {"stream_all": true|false} switches every line to streaming. Its render starts at once."""
    _admin(request, token)
    try:
        body = json.loads(await request.body() or b"{}")
        assert isinstance(body, dict)
    except Exception:  # noqa: BLE001
        raise HTTPException(status_code=400, detail="a JSON object")
    lib = LIB.get()
    data = json.loads((LIB_DIR / "revoice.json").read_text(encoding="utf-8")) if (LIB_DIR / "revoice.json").exists() \
        else {"stream_all": False, "lines": []}
    data.setdefault("lines", [])
    if "stream_all" in body:
        data["stream_all"] = bool(body["stream_all"])
    entry = None
    text, key = str(body.get("text") or "").strip(), str(body.get("key") or "").strip().lower()
    if key and not text:
        text = _stock_lines(lib).get(key, "")
        if not text:
            raise HTTPException(status_code=404, detail="no stock line has that key: send its text")
    if text:
        text = say_clean(text)
        c = lib.cfg
        entry = {"text": text, "key": voice_key(c["voice"], c["voice_speed"], text),
                 "alt": voice_key(c["fallback_voice"], c["voice_speed"], text),
                 "why": str(body.get("why") or "")[:200], "at": time.strftime("%Y-%m-%d %H:%M", time.gmtime())}
        data["lines"] = [e for e in data["lines"] if e.get("text") != text] + [entry]
    _write_lib("revoice", data)
    rendering = _prerender_start([entry["text"]]) if entry and body.get("render", True) else False
    return {"ok": True, "line": entry, "stream_all": data.get("stream_all", False), "rendering": rendering,
            "error": LIB.error}


@router.delete("/edith/brain/revoice/{key}")
async def revoice_delete(key: str, request: Request, token: Optional[str] = None):
    """Unmark a line (by its key, alt, or exact text): the bank copy plays again."""
    _admin(request, token)
    data = json.loads((LIB_DIR / "revoice.json").read_text(encoding="utf-8"))
    lines = data.get("lines", [])
    keep = [e for e in lines if key not in (e.get("key"), e.get("alt"), e.get("text"))]
    if len(keep) == len(lines):
        raise HTTPException(status_code=404, detail="not on the list")
    data["lines"] = keep
    _write_lib("revoice", data)
    return {"ok": True, "removed": len(lines) - len(keep)}


@router.post("/edith/brain/revoice/render")
async def revoice_render(request: Request, token: Optional[str] = None):
    """Pre-render every re-voiced line whose render isn't cached yet. Runs in the background;
    GET /edith/brain/revoice shows the progress."""
    _admin(request, token)
    todo = [e["text"] for e in LIB.get().revoice if e.get("text") and not _rendered(e["text"])]
    return {"ok": True, "queued": len(todo) if _prerender_start(todo) else 0, "busy": _prerender["running"]}


@router.get("/edith/brain/lines")
async def brain_lines(request: Request, token: Optional[str] = None, q: str = ""):
    """Her stock lines with their pack keys (key: main voice, alt: fallback), to find a broken one."""
    _admin(request, token)
    lib = LIB.get()
    c = lib.cfg
    seen, out = set(), []
    for t in _stock_lines(lib).values():
        if t in seen or (q and q.lower() not in t.lower()):
            continue
        seen.add(t)
        out.append({"text": t, "key": voice_key(c["voice"], c["voice_speed"], t),
                    "alt": voice_key(c["fallback_voice"], c["voice_speed"], t),
                    "revoiced": lib.must_stream([voice_key(c["voice"], c["voice_speed"], t)])})
    return out[:500]


@router.get("/edith/brain/unknown", response_class=PlainTextResponse)
async def brain_unknown(request: Request, token: Optional[str] = None):
    """What she couldn't answer (what to add to her library next)."""
    _admin(request, token)
    f = DATA / "unknown.txt"
    return f.read_text(encoding="utf-8")[-20000:] if f.exists() else ""


@router.get("/edith/brain/reports")
async def brain_reports(request: Request, token: Optional[str] = None):
    """What she reported (toxic players, bug reports), newest last."""
    _admin(request, token)
    return self_rec()["reports"]


@router.get("/edith/brain/learned")
async def brain_learned(request: Request, token: Optional[str] = None):
    """What she learned by herself (her chat model's answers): candidates for her voice banks."""
    _admin(request, token)
    return sorted(self_rec()["auto"], key=lambda e: -(e.get("uses", 0) + 2 * e.get("good", 0)))


@router.delete("/edith/brain/learned/{eid}")
async def brain_learned_delete(eid: str, request: Request, token: Optional[str] = None):
    _admin(request, token)
    e = auto_find(eid)
    if not e:
        raise HTTPException(status_code=404, detail="no such answer")
    self_rec()["auto"].remove(e)
    auto_changed()
    return {"ok": True}


@router.get("/edith/brain/player/{name}")
async def brain_player(name: str, request: Request, token: Optional[str] = None):
    _admin(request, token)
    return store().get(name)


@router.put("/edith/brain/player/{name}")
async def brain_player_put(name: str, request: Request, token: Optional[str] = None):
    """Replace a player's record (importing an old memory.json: its keys are the same)."""
    _admin(request, token)
    try:
        data = json.loads(await request.body())
        assert isinstance(data, dict)
    except Exception:  # noqa: BLE001
        raise HTTPException(status_code=400, detail="a JSON object")
    rec = blank_record()
    rec.update({k: v for k, v in data.items() if k in rec})
    store().recs[name] = rec
    PEOPLE.pop(name, None)
    store().mark(name)
    store().write(store().take())
    return {"ok": True}


@router.delete("/edith/brain/player/{name}")
async def brain_player_delete(name: str, request: Request, token: Optional[str] = None):
    _admin(request, token)
    store().forget(name)
    PEOPLE.pop(name, None)
    return {"ok": True}

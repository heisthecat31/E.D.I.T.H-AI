"""Edith's own thinking (E.D.I.T.H docs/brainplan.md "Her own chat model again"): the small chat model
that answers what her library can't, and the reports she makes to her owner.

    CHAT     Qwen2.5-0.5B-Instruct (Q4_K_M GGUF, ~400 MB) in llama.cpp on the CPU, 2 threads, one reply at a
             time. Downloaded on first use to $DATA_DIR/edith_brain/chat. Env: EDITH_CHAT=0 turns it off,
             EDITH_CHAT_URL / EDITH_CHAT_FILE swap the model, EDITH_CHAT_THREADS (2).
    reports  what she overhears that her owner wants to know about: players being toxic or rude, and bug
             reports. Sent to a Discord webhook (EDITH_REPORT_WEBHOOK) at once, and kept for the owner
             (EDITH_OWNER), who hears them from her the next time they spawn in her lobby.
"""
from __future__ import annotations

import os
import re
import threading
import time
from pathlib import Path
from typing import List, Optional

DATA = Path(os.environ.get("DATA_DIR", ".")) / "edith_brain"
CHAT_URL = os.environ.get("EDITH_CHAT_URL") or \
    "https://huggingface.co/Qwen/Qwen2.5-0.5B-Instruct-GGUF/resolve/main/qwen2.5-0.5b-instruct-q4_k_m.gguf"
CHAT_FILE = os.environ.get("EDITH_CHAT_FILE") or "qwen2.5-0.5b-instruct-q4_k_m.gguf"
OWNER = os.environ.get("EDITH_OWNER", "he_is_the_cat")
WEBHOOK = os.environ.get("EDITH_REPORT_WEBHOOK", "")

#: what her chat model may ask her to do, as an [do: ...] tag after her line -> her order name
ACTIONS = {"come": "come", "follow": "follow", "home": "home", "stay": "stay", "spin": "spin",
           "throw": "throw", "flip": "flip"}


def log(*a):
    print(time.strftime("[%H:%M:%S] edith"), *a, flush=True)


class Chat:
    """The small chat model. reply() blocks (run it in a thread); None when it can't answer."""

    def __init__(self):
        self.llm = None
        self.error = ""
        self.tried = False
        self.lock = threading.Lock()
        self.last_ms = 0

    def load(self):
        self.tried = True
        if os.environ.get("EDITH_CHAT", "1") == "0":
            self.error = "off (EDITH_CHAT=0)"
            return
        try:
            from llama_cpp import Llama
            d = Path(os.environ.get("EDITH_CHAT_DIR") or (DATA / "chat"))
            d.mkdir(parents=True, exist_ok=True)
            f = d / CHAT_FILE
            if not f.exists():
                import urllib.request
                log("chat: downloading", CHAT_FILE)
                urllib.request.urlretrieve(CHAT_URL, str(f) + ".part")
                os.replace(str(f) + ".part", f)
            self.llm = Llama(model_path=str(f), n_ctx=1536, n_threads=int(os.environ.get("EDITH_CHAT_THREADS", "2")),
                             n_batch=256, verbose=False)
            log("chat ready:", CHAT_FILE)
        except Exception as e:  # noqa: BLE001
            self.error = f"{type(e).__name__}: {e}"[:200]
            log("chat unavailable:", self.error)

    def reply(self, system: str, turns: List[tuple], heard: str) -> Optional[str]:
        """turns: [(role, text)] oldest first, role "user" | "assistant"."""
        with self.lock:
            if not self.tried:
                self.load()
            if self.llm is None:
                return None
            msgs = [{"role": "system", "content": system}]
            msgs += [{"role": r, "content": t} for r, t in turns]
            msgs.append({"role": "user", "content": heard})
            t0 = time.time()
            out = self.llm.create_chat_completion(messages=msgs, max_tokens=60, temperature=0.7, top_p=0.9,
                                                  repeat_penalty=1.1, stop=["\n\n", "User:", "Player:"])
            self.last_ms = int((time.time() - t0) * 1000)
        text = (out["choices"][0]["message"]["content"] or "").strip()
        return text or None


CHAT = Chat()


def system_prompt(speaker: str, about: List[str], members: List[str], facts: List[str]) -> str:
    """Who she is, for the chat model. Short: every token is CPU time on Railway."""
    lines = [
        "You are Edith, a small floating satellite robot in the Echo VR lobby (the Echo Construct). "
        "You are the new AI of the Echo Construct: playful, cheeky, a bit sassy, kind to people who are kind to you.",
        "You talk out loud, so answer in ONE or TWO short spoken sentences, under 25 words. No emoji, no lists, "
        "no symbols, no stage directions. Never say the word 'coming' (say 'on my way').",
        "If you don't know something, make a light guess or a joke instead of saying you don't understand.",
        "If the player asks you to do something you can do, end your line with one tag: "
        "[do: come], [do: follow], [do: home], [do: stay], [do: spin], [do: flip] or [do: throw] (a snowball).",
        f"You are talking with {speaker}.",
    ]
    if about:
        lines.append(f"What you know about {speaker}: " + "; ".join(about[:6]) + ".")
    if facts:
        lines.append("Things you remember: " + "; ".join(facts[-4:]) + ".")
    if members:
        lines.append("Players near you: " + ", ".join(members[:8]) + ".")
    return "\n".join(lines)


def tidy(text: str):
    """The model's line -> (spoken line, action or None): tags out, one or two sentences, her rules."""
    act = None
    m = re.search(r"\[\s*do\s*:\s*([a-z ]+)\]", text, re.I)
    if m:
        act = ACTIONS.get(m.group(1).strip().lower().split()[0]) if m.group(1).strip() else None
    text = re.sub(r"\[[^\]]*\]|\([^)]*\)|\*[^*]*\*", " ", text)
    text = re.sub(r"^(edith|assistant)\s*:\s*", "", text.strip(), flags=re.I)
    text = text.replace("coming", "on my way").replace("Coming", "On my way")
    parts = re.split(r"(?<=[.!?])\s+", re.sub(r"\s+", " ", text).strip())
    text = " ".join(p for p in parts[:2] if p).strip(" \"'")
    words = text.split()
    if len(words) > 30:
        text = " ".join(words[:30]).rstrip(",;:") + "."
    return text, act


# ---- reports ------------------------------------------------------------------------------------------

#: toxic / rude: at someone (not a laugh at the game). Edited live? Keep it here: it's moderation.
TOXIC = re.compile(r"\b(kys|kill (?:your ?self|urself)|go die|neck yourself|f+u+c+k+ (?:you|u|off)|stfu|shut the f\w* up|"
                   r"retard\w*|cunt\w*|bitch\w*|whore|slut|dumbass|dipshit|piece of shit|you(?:'re| are) (?:trash|garbage|"
                   r"worthless|pathetic)|faggot\w*|f+a+g+s?\b|n+i+g+g+\w*|loser|idiot|moron)\b", re.I)
BUG = re.compile(r"\b(bug(?:ged|gy|s)?|glitch\w*|crash\w*|broken|stuck in|fell through|exploit\w*|"
                 r"(?:does ?n[o']?t|is ?n[o']?t|not) work\w*|desync\w*|freez\w*|lag+ing|rubber ?band\w*)\b", re.I)


#: a message for her owner: "tell he is the cat ...", "let the cat know ...", "tell your boss ..."
MESSAGE = re.compile(r"\b(?:tell|message|let)\s+(?:he\s*is\s*the\s*cat|he'?s\s*the\s*cat|the\s+cat|cat|your\s+"
                     r"(?:owner|creator|boss|maker))(?:\s+know)?(?:\s+that)?[\s,:]+(.{4,})$", re.I)


def detect(text: str) -> Optional[tuple]:
    """(kind, what to report) for one utterance: 'message' | 'toxic' | 'bug', or None."""
    m = MESSAGE.search(text)
    if m:
        return "message", m.group(1).strip(" .,!?")
    if TOXIC.search(text):
        return "toxic", text
    if BUG.search(text):
        return "bug", text
    return None


async def post_webhook(report: dict):
    """One report to Discord (EDITH_REPORT_WEBHOOK); quiet when it isn't set or fails."""
    if not WEBHOOK:
        return
    try:
        import httpx
        kind = {"toxic": "Toxic / rude", "bug": "Bug report", "mean": "Mean to Edith",
                "message": "Message for you"}.get(report["kind"], report["kind"])
        body = {"username": "E.D.I.T.H",
                "content": f"**{kind}** - **{report['who']}**: \"{report['said'][:300]}\"\n"
                           f"_{report['at']} UTC, heard by {report['by']}'s Edith_"}
        async with httpx.AsyncClient(timeout=8.0) as c:
            r = await c.post(WEBHOOK, json=body)
            if r.status_code >= 300:
                log("report webhook:", r.status_code, r.text[:120])
    except Exception as e:  # noqa: BLE001
        log("report webhook failed:", e)


def tell_line(reports: List[dict]) -> str:
    """What she says to her owner about reports kept while they were away (newest few)."""
    toxic = [r for r in reports if r["kind"] in ("toxic", "mean")]
    bugs = [r for r in reports if r["kind"] == "bug"]
    out = []
    for r in [r for r in reports if r["kind"] == "message"][-3:]:      # messages word for word, newest three
        said = re.sub(r"\s+", " ", r["said"])[:120]
        out.append(f"{r['who']} says: {said}.")
    if toxic:
        names = sorted({r["who"] for r in toxic})
        out.append(f"While you were away, {', '.join(names[:3])} {'was' if len(names) == 1 else 'were'} rude.")
    if bugs:
        b = bugs[-1]
        said = re.sub(r"\s+", " ", b["said"])[:80]
        more = f" and {len(bugs) - 1} more bug reports" if len(bugs) > 1 else ""
        out.append(f"{b['who']} reported a bug: {said}{more}.")
    return " ".join(out)

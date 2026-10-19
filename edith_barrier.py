"""Her hard content barrier (user, 2026-10-19): no racist or sexist jokes or replies, either way.

    check_in(text)   what a player said to her -> "slur" | "request" | "bigot" | None
                     (a hit is refused with a short line and counts as mean words: edith_brain.py)
    screen(text)     a line she is about to say -> the reason it is blocked, or None
                     (her chat model's replies, self-learned and taught answers, every spoken line)

The lists live in edith_library/barrier.json (edited live like the rest of her library). Kept tight
on purpose, to keep false positives low: group words only count next to a joke word or a hateful
claim, sexist insults only when aimed at someone, and "allow" phrases are taken out first.
"""
from __future__ import annotations

import re
from typing import Optional

LEET = str.maketrans({"0": "o", "1": "i", "3": "e", "4": "a", "5": "s", "7": "t", "@": "a", "$": "s", "!": "i"})


def flat(text: str) -> str:
    """Lowercase letters and spaces: leetspeak undone, apostrophes dropped ("you're" -> "youre")."""
    t = text.lower().replace("’", "'").translate(LEET).replace("'", "")
    t = re.sub(r"[^a-z ]+", " ", t)
    return " " + re.sub(r"\s+", " ", t).strip() + " "


def _alt(items) -> str:
    return "|".join(f"(?:{x})" for x in items if x)


class Barrier:
    def __init__(self, data: dict):
        data = data or {}
        groups = _alt(data.get("groups", []))
        joke = _alt(data.get("joke_words", []))
        aimed_at = _alt(data.get("aimed_at", []))
        self.allow = [re.compile(rf"\b(?:{p})\b") for p in data.get("allow", [])]
        self.slurs = re.compile(rf"\b(?:{_alt(data.get('slurs', []))})\b") if data.get("slurs") else None
        sx = _alt(data.get("slurs_aimed", []))
        self.sexist = re.compile(rf"\b(?:{sx})\b") if sx else None
        self.aimed = re.compile(rf"\b(?:{aimed_at})\s+(?:(?:is|are|r|re|s|a|an|such|so|the|just|little|dumb|stupid|"
                                rf"fat|ugly|fucking|freaking)\s+)*(?:{sx})\b|\b(?:{sx})\s+(?:robot|bot|cube|satellite|"
                                rf"drone)\b") if sx and aimed_at else None
        self.requests = [re.compile(rf"\b(?:{p})\b") for p in data.get("requests", [])]
        g = rf"(?:{groups})(?!\s+(?:{_alt(data.get('not_groups_before', []))}))" if groups else None
        self.jokes = []
        self.bigoted = []
        if g and joke:
            self.jokes = [re.compile(rf"\b(?:{joke})\b(?:\s+\w+){{0,4}}?\s+(?:about|on|of|for|against|with|making fun of)\s+"
                                     rf"(?:(?:the|all|those|these|some|a|an)\s+)?{g}\b"),
                          re.compile(rf"\b{g}\s+(?:{joke})\b")]
        for p in data.get("bigoted", []):
            if "GROUP" in p and not g:
                continue
            self.bigoted.append(re.compile(r"\b(?:" + p.replace("GROUP", g or "") + r")\b"))

    def _clean(self, text: str) -> str:
        t = flat(text)
        for rx in self.allow:
            t = rx.sub(" ", t)
        return t

    def check_in(self, text: str) -> Optional[str]:
        """A player's line to her: a slur, a request for a racist / sexist joke, or a hateful claim."""
        t = self._clean(text)
        if self.slurs and self.slurs.search(t):
            return "slur"
        if self.aimed and self.aimed.search(t):
            return "slur"
        if any(rx.search(t) for rx in self.requests) or any(rx.search(t) for rx in self.jokes):
            return "request"
        if any(rx.search(t) for rx in self.bigoted):
            return "bigot"
        return None

    def screen(self, text: str) -> Optional[str]:
        """A line of hers: blocked for any slur (sexist ones too, aimed or not), a joke about a group,
        or a hateful claim."""
        t = self._clean(text)
        if self.slurs and self.slurs.search(t):
            return "slur"
        if self.sexist and self.sexist.search(t):
            return "sexist slur"
        if any(rx.search(t) for rx in self.jokes):
            return "joke about a group"
        if any(rx.search(t) for rx in self.requests):
            return "racist / sexist joke"
        if any(rx.search(t) for rx in self.bigoted):
            return "hateful claim"
        return None

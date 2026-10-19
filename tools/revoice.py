"""Her re-voice list from the command line: lines whose voice-bank copy in EDITH.dll is broken, so the
game streams the server's render (Kokoro af_heart) instead.

    python tools/revoice.py find <words>          her stock lines containing <words>, with their keys
    python tools/revoice.py mark "<line text>"    mark a line broken (its render starts at once)
    python tools/revoice.py mark-key <key>        the same, by a pack key or alt (a stock line's)
    python tools/revoice.py unmark <key|text>     the bank copy plays again
    python tools/revoice.py list                  what's marked, and whether each render is cached
    python tools/revoice.py render                pre-render every marked line not cached yet
    python tools/revoice.py all on|off            stream every line (the banks ignored) / back to normal

EDITH_URL (default https://edith.echovr.de) and ADMIN_TOKEN from the environment.
"""
import json
import os
import sys
import urllib.parse
import urllib.request

BASE = os.environ.get("EDITH_URL", "https://edith.echovr.de").rstrip("/") + "/edith/brain/"
TOKEN = os.environ.get("ADMIN_TOKEN", "")


def call(method, path, body=None):
    req = urllib.request.Request(BASE + path, method=method, data=None if body is None else json.dumps(body).encode(),
                                 headers={"x-admin-token": TOKEN, "content-type": "application/json"})
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read().decode() or "null")


def main(argv):
    if not argv or not TOKEN:
        print(__doc__)
        return 2
    cmd, rest = argv[0], " ".join(argv[1:])
    if cmd == "find":
        for l in call("GET", "lines?q=" + urllib.parse.quote(rest)):
            print(f"{l['key']} {l['alt']} {'[revoiced] ' if l['revoiced'] else ''}{l['text']}")
    elif cmd == "mark":
        print(call("POST", "revoice", {"text": rest}))
    elif cmd == "mark-key":
        print(call("POST", "revoice", {"key": rest}))
    elif cmd == "unmark":
        print(call("DELETE", "revoice/" + urllib.parse.quote(rest, safe="")))
    elif cmd == "list":
        print(json.dumps(call("GET", "revoice"), indent=1))
    elif cmd == "render":
        print(call("POST", "revoice/render"))
    elif cmd == "all" and rest in ("on", "off"):
        print(call("POST", "revoice", {"stream_all": rest == "on"}))
    else:
        print(__doc__)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

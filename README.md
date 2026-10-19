# E.D.I.T.H-AI

The server side of **E.D.I.T.H**, the satellite in the Echo VR lobby (the game side is the
[E.D.I.T.H](https://github.com/heisthecat31/E.D.I.T.H) plugin, `EDITH.dll`). It is her brain and her relay:

- **Relay** (`edith_relay.py`, `wss://<host>/edith/ws`): one shared Edith per Echo match. Players running
  EDITH.dll are grouped by match; one game hosts her and the rest follow its state.
- **Brain** (`edith_brain.py`, `wss://<host>/edith/brain`): what each game's built-in ears send her.
  - Hearing: Whisper (faster-whisper `base.en`, int8, CPU) turns players' speech into text.
  - Her rules: the wake word, orders (come, follow, spin, stay, home, throw), stubbornness and
    points per player, her favourite, teaching ("when someone says X, say Y"), per-player memory.
  - Her library (`edith_library/*.json`): instant lines, knowledge, small talk, reactions. Editable
    live over HTTP.
- **Her own chat model** (`edith_mind.py`): what her library can't answer goes to a small local
  model, Qwen2.5-0.5B-Instruct in llama.cpp on the CPU (~0.9 s a reply measured locally). It runs off
  the event loop, so other players never wait on it. She keeps its replies as answers she learned
  herself: the same question next time is instant, and replies she gets told off for are dropped.
  It can also carry out what a player asks with an action tag (come, follow, spin, ...).
- **Her voice**: lines in her voice banks (built into EDITH.dll) play instantly; new lines are
  rendered here with Kokoro (`af_heart`) and streamed to the game.
- **Reports**: players being toxic or rude, and bug reports she overhears, go to a Discord webhook
  at once and are kept for her owner, who hears them from her the next time they spawn in her lobby.

## Run it (Docker, Debian)

```sh
git clone https://github.com/heisthecat31/E.D.I.T.H-AI.git
cd E.D.I.T.H-AI
cp .env.example .env        # fill in EDITH_OWNER, EDITH_REPORT_WEBHOOK, ADMIN_TOKEN
docker compose up -d --build
```

That serves her on `127.0.0.1:8000`. EDITH.dll speaks `wss://`, so players' games need HTTPS on a
hostname. Pick one:

- **Public IP + domain**: point `EDITH_HOST` (in `.env`) at the machine, open ports 80 and 443, then
  `docker compose --profile caddy up -d --build`. Caddy gets the certificate.
- **Cloudflare Tunnel** (no open ports): create a tunnel whose public hostname points at
  `http://edith:8000`, put its token in `TUNNEL_TOKEN`, then `docker compose --profile tunnel up -d --build`.

First start downloads Whisper (~145 MB), Kokoro (~120 MB) and the chat model (~400 MB) into the
`edith-data` volume. They're kept there, along with her memory, library, learned answers and reports.

Sizing: about 1.5 GB of RAM in use; 4 CPU cores are comfortable. Thread counts are in `.env`.

Point EDITH.dll at it with its `relay` and `brain` settings (nEVR: the launcher's Options for E.D.I.T.H;
otherwise `EDITH.ini` beside the DLL):

```ini
[satellite]
relay=wss://edith.example.com/edith/ws
brain=wss://edith.example.com/edith/brain
```

## Run it without Docker

```sh
pip install -r requirements.txt
DATA_DIR=./data uvicorn edith_app:app --host 127.0.0.1 --port 8000
python tools/test_brain.py http://127.0.0.1:8000
```

`tools/test_brain.py` plays a game's part with typed lines: questions for her chat model, an order,
a toxic line and a bug report from another player, then your spawn (she tells you the reports).

## Settings (environment)

| | |
|---|---|
| `EDITH_OWNER` | who she reports to (in-game name) |
| `EDITH_REPORT_WEBHOOK` | Discord webhook URL for reports (empty: in game only) |
| `ADMIN_TOKEN` | for the admin endpoints below |
| `DATA_DIR` | where her data lives (`/data` in Docker) |
| `DATABASE_URL` | optional Postgres for player records (else JSON files in `DATA_DIR`) |
| `EDITH_STT=0` / `EDITH_TTS=0` / `EDITH_CHAT=0` | turn Whisper / Kokoro / the chat model off |
| `EDITH_WHISPER_THREADS`, `EDITH_VOICE_THREADS`, `EDITH_CHAT_THREADS` | CPU threads each |
| `EDITH_CHAT_URL`, `EDITH_CHAT_FILE` | another GGUF chat model |

## Admin endpoints (`?token=` or `x-admin-token` header)

| | |
|---|---|
| `GET /edith/brain/stats` | (no token) companions connected, which models are up |
| `GET /edith/brain/reports` | what she reported |
| `GET /edith/brain/learned` | what she learned by herself, most used first |
| `DELETE /edith/brain/learned/{id}` | forget one |
| `GET`/`PUT /edith/brain/library/{name}` | her library files, edited live |
| `GET /edith/brain/unknown` | what nothing answered |
| `GET`/`PUT`/`DELETE /edith/brain/player/{name}` | a player's record |

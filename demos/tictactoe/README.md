# ❌⭕ tic-tac-toe (demo)

The smallest complete colyseus-rs game: two players, chat, reconnection —
with a full T3-stack frontend (Next.js + tRPC + NextAuth + Drizzle/SQLite).

```
demos/tictactoe/
  server/   # Rust game backend (colyseus-rs) — port 2567
  web/      # T3 app — port 3000
```

## Run it manually

```sh
# 0) one-time: deps + database
cd demos/tictactoe/web && npm install && npm run db:push

# 1) game backend (terminal 1)
cargo run -p tictactoe-server

# 2) web app (terminal 2)
cd demos/tictactoe/web && npm run dev
```

Open `http://localhost:3000` in two browsers/profiles, sign up two users,
one clicks **+ new game**, the other joins from the lobby list. First joiner
is **X**, second is **O**. Chat and rematch included.

## Configuration

| env var | where | default | meaning |
| --- | --- | --- | --- |
| `GAME_SECRET` | both | `dev-secret-change-me` | shared HS256 secret for game tokens |
| `NEXT_PUBLIC_GAME_URL` | web | `http://localhost:2567` | game server URL for browsers |

## What it showcases

- Room lifecycle: `on_create / on_auth / on_join / on_drop / on_reconnect /
  on_leave`, `max_clients = 2`
- **Auth flow**: the web app issues a short-lived HS256 JWT (tRPC
  `game.gameToken`), the browser presents it as `Authorization: Bearer …` on
  matchmaking, and the room verifies it in `on_auth` with the shared
  `GAME_SECRET` — the display name comes from the token.
- **State sync**: the board/turn/status are a plain `Serialize` struct;
  clients receive full state on join and JSON-Patch deltas after.
- **Chat as messages** (not state): ephemeral `chat`/`system` broadcasts.
- **Reconnection**: 60s `allow_reconnection` on drop + the client persists
  the reconnection token in `sessionStorage`, so F5 resumes the seat.
- **Session takeover**: joining with the same account under a new session
  moves the seat (and symbol) over via `RoomContext::remove_client`.

The game logic lives in a single `server/src/main.rs` (~250 lines) — read it
as the "hello world" of the framework, then look at `demos/trivia` for the
command-pattern structure.

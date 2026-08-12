# demos

Two full-stack demo games built on colyseus-rs. Each has a Rust game backend
(`server/`) and a T3-stack web frontend (`web/` — Next.js + tRPC + NextAuth +
Drizzle/SQLite).

| demo | port(s) | showcases |
| --- | --- | --- |
| [`tictactoe`](tictactoe) | game `:2567`, web `:3000` | room basics, JWT auth into `on_auth`, F5-proof reconnection, session takeover |
| [`trivia`](trivia) | game `:2568`, web `:3001` | command pattern, LLM background tasks via `RoomSender`, per-round timers, spectators, internal global chat room, Redis-fed leaderboard, admin panel |
| [`mapguesser`](mapguesser) | game `:2569`, web `:3002` | single global never-disposing internal room, anonymous/guest auth tokens, endless round loop, LLM hints, mobile-first map UI (Leaflet), mid-game joins |

Both share the same env convention: `GAME_SECRET` (HS256 game tokens between
web and game server) must match on both sides — the dev default is
`dev-secret-change-me` everywhere, so local setup needs no changes.

Quick start (per demo, details in each README):

```sh
# game backend
cargo run -p tictactoe-server      # or: cargo run -p trivia-server / cargo run -p mapguesser-server

# web frontend
cd demos/tictactoe/web             # or: demos/trivia/web / demos/mapguesser/web
npm install && npm run db:push     # first time only
npm run dev                        # tictactoe; trivia: -- --port 3001; mapguesser: -- --port 3002
```

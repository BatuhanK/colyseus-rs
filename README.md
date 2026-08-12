# colyseus-rs

A multiplayer game server framework for **Rust**, inspired by
[Colyseus](https://colyseus.io/) — rethought for Rust instead of ported
line-by-line. Rooms, matchmaking, seat reservations, state synchronization,
reconnection, timers and a game loop, on top of tokio + axum.

> Not compatible with the colyseus.js client — the protocol is documented
> below and a minimal TypeScript client lives in [`clients/ts`](clients/ts).

## Why it can be faster than the original

- **Every room is an actor**: each room runs on its own tokio task and all of
  its handlers execute sequentially. No locks, no event-emitter overhead, no
  GC pauses — and rooms spread across cores for free.
- **Zero-copy-ish messaging**: outbound frames are encoded once per broadcast
  and shared as `Bytes` across all client writer tasks.
- **serde-native state sync**: instead of `@colyseus/schema` (decorators,
  reflection, custom binary format), *any* `#[derive(Serialize)]` struct can
  be room state. The framework diffs snapshots and broadcasts **JSON-Patch**
  (RFC 6902) deltas, encoded as MessagePack.

## Quick start

```rust
use colyseus::serde_json::{json, Value};
use colyseus::{async_trait, Client, Result, Room, RoomContext, Server};
use serde::{Deserialize, Serialize};

#[derive(Serialize)]
struct GameState { score: i64 }

struct GameRoom;

#[derive(Deserialize)]
struct Increment { by: i64 }

#[async_trait]
impl Room for GameRoom {
    async fn on_create(&mut self, ctx: &mut RoomContext, _options: Value) -> Result<()> {
        ctx.set_state(GameState { score: 0 });
        ctx.set_max_clients(Some(8));

        ctx.on_message("increment", |_room: &mut GameRoom, ctx, _client, msg: Increment| {
            Box::pin(async move {
                ctx.state_mut::<GameState>().unwrap().score += msg.by;
                Ok(())
            })
        });
        Ok(())
    }

    async fn on_join(&mut self, ctx: &mut RoomContext, client: Client, _o: Value, _a: Option<Value>) -> Result<()> {
        ctx.broadcast("system", &json!({ "text": format!("{} joined", client.session_id()) }));
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    let mut server = Server::new();
    server.define("game", || GameRoom)
          .filter_by(&["mode"])
          .sort_by(&[("clients", 1)]);
    server.listen("0.0.0.0:2567").await.unwrap();
}
```

Runnable examples:

```sh
cargo run --example chat    # broadcast chat room
cargo run --example game    # state sync, game loop, reconnection
```

## Feature map (vs. Colyseus)

| Capability | Colyseus | colyseus-rs |
| --- | --- | --- |
| Room lifecycle (`on_create/join/leave/drop/reconnect/dispose`) | ✅ | ✅ |
| `on_auth` per room | ✅ (static) | ✅ (called at seat reservation) |
| Matchmaking: `joinOrCreate` / `create` / `join` / `joinById` / `reconnect` | ✅ | ✅ |
| Seat reservations w/ timeout | ✅ | ✅ |
| `filter_by` / `sort_by` / default options | ✅ | ✅ |
| Concurrent-creation lock (same criteria → one room) | ✅ | ✅ |
| `max_clients`, `lock/unlock`, `set_private`, metadata | ✅ | ✅ |
| State sync | Schema (custom binary) | any `Serialize` struct → JSON-Patch + msgpack |
| Patch rate / manual `broadcast_patch` | ✅ | ✅ |
| Simulation interval (`on_tick`), `Clock` | ✅ | ✅ |
| `set_timeout` timers | ✅ | ✅ |
| `allow_reconnection` (timed or manual) | ✅ | ✅ |
| `after_next_patch` send/broadcast option | ✅ | ✅ |
| String & numeric message types, binary messages | ✅ | ✅ |
| Catch-all message handler | `'*'` | `on_any_message` |
| Message rate limiting | ✅ | ✅ (`max_messages_per_second`) |
| Auto-dispose | ✅ | ✅ (immediate after last leave; grace period for empty new rooms) |
| Lobby listing events | `LobbyRoom` | `MatchMaker::subscribe()` + `GET /rooms` |
| Per-client state filtering | `@view()` | ✅ `RoomContext::set_view_filter` |
| Command pattern | `@colyseus/command` | ✅ built-in (`Command` / `Dispatcher` / `Dispatchable`) |
| Background tasks into rooms | (ad-hoc) | ✅ `RoomSender` (`ctx.sender()`) |
| Presence (pub/sub + KV) | Local / Redis | `Presence` trait + in-memory impl |
| Multi-process (Redis driver + IPC) | ✅ | 🔜 trait seams exist, not implemented |
| Graceful shutdown | ✅ | ✅ (SIGINT/SIGTERM → dispose all rooms) |
| Admin panel / monitor | `@colyseus/monitor` | ✅ built-in (`/admin`) |
| `devMode` room restore | ✅ | 🔜 |

## Architecture

```
┌──────────────────────────── Server (axum) ───────────────────────────┐
│ POST /matchmake/{method}/{roomName} ──► MatchMaker                    │
│ GET  /rooms[/{name}]                 ──► LocalDriver (listings)       │
│ GET  /ws/{roomId}?sessionId=…        ──► Room actor (via mailbox)     │
└───────────────────────────────────────────────────────────────────────┘
        MatchMaker ── owns ──► RoomHandle { room_id, mpsc::Sender }
        LocalDriver ── RoomListing { roomId, name, clients, locked, … }
        Presence    ── pub/sub + key-value (trait; in-memory default)

Each room = one tokio task:
  select! { mailbox events │ simulation interval │ patch interval │ timers }
  → all user handlers get &mut Room + &mut RoomContext, sequentially.
```

### The room actor contract

- `Room` handlers are never called concurrently — room code needs no `Mutex`.
- Matchmaking RPCs (`ReserveSeat`, `Connect`, …) and client messages travel
  through the room's mailbox.
- Client → room messages carry a *connection id*, so stale sockets from before
  a reconnection can't interfere with the new connection.

## Wire protocol (for mobile / custom clients)

Transport: WebSocket, binary frames, [MessagePack](https://msgpack.org/).
A frame is a msgpack **array** whose first element is a code; the only
exception is ping, a single byte `0x12`.

**Handshake flow**

1. `POST /matchmake/{method}/{roomName}` with a JSON body of client options →
   `200` with `{ room: { roomId, … }, sessionId, processId, … }`.
2. Open `GET /ws/{roomId}?sessionId={sessionId}` (add
   `&reconnectionToken={token}` when reconnecting).
3. Server sends `[10, reconnectionToken, serializerId]` — you are joined.
   If the room has state, `[14, fullState]` follows immediately.

**Server → client**

| frame | meaning |
| --- | --- |
| `[10, token, serializerId]` | join handshake (keep the token for reconnection!) |
| `[11, code, message]` | error |
| `[13, type, payload]` | room message |
| `[14, state]` | full room state |
| `[15, patch]` | JSON-Patch (RFC 6902) array; apply to your state copy |
| `[17, type, bytes]` | room message with binary payload |
| `0x12` (single byte) | ping/pong |

**Client → server**

| frame | meaning |
| --- | --- |
| `[13, type, payload]` | room message (`type`: string or integer) |
| `[17, type, bytes]` | room message with binary payload |
| `[12]` | consented leave |
| `0x12` (single byte) | ping |

**Reconnection**: server code calls `ctx.allow_reconnection(&client, timeout)`
inside `on_drop`. The client then `POST /matchmake/reconnect/{roomId}` with
`{ "reconnectionToken": "<token from handshake>" }` and opens the WebSocket
with both `sessionId` and `reconnectionToken` query params. A fresh full
state is sent after rejoining.

**HTTP errors**: matchmaking failures return the Colyseus-style codes as HTTP
status (`520` no handler, `521` invalid criteria, `522` invalid room id,
`524` expired, `525` auth failed, …) with a `{ code, error }` JSON body.

## Per-client state views (`@view()` counterpart)

By default a room's state patch is computed once and broadcast to everyone.
When players must not see each other's data (hidden hands, fog of war),
register a projection:

```rust
async fn on_create(&mut self, ctx: &mut RoomContext, _o: Value) -> Result<()> {
    ctx.set_state(CardGame::default());
    ctx.set_view_filter(|state: &CardGame, client: &Client| PlayerView {
        table: state.table.clone(),
        my_hand: state.hands[client.session_id()].clone(),
        opponent_card_count: state.opponent_count(client.session_id()),
    });
    Ok(())
}
```

Each client then receives the full *view* on join and JSON-Patches computed
against their own view afterwards. Cost note: views serialize + diff once per
**client** per patch tick (rooms without a filter pay once per room), so keep
projections cheap and prefer them in smaller rooms.

## Command pattern (`@colyseus/command` counterpart)

Built into the framework — no separate package needed. Commands decouple
*what triggers* game logic (message handlers, timers, lifecycle hooks) from
*how it executes*, and each command is a small, independently unit-testable
struct:

```rust
use colyseus::{BoxFuture, Command, Dispatchable, Dispatcher, Room, RoomContext};

struct DealCards { count: usize }

impl Command<PokerRoom> for DealCards {
    fn execute<'a>(
        self: Box<Self>,
        room: &'a mut PokerRoom,
        ctx: &'a mut RoomContext,
        dispatcher: &'a mut Dispatcher<PokerRoom>,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            // ... mutate room / ctx.state ...
            dispatcher.enqueue(CheckWinner); // chain follow-up commands (FIFO)
        })
    }
}

struct PokerRoom { dispatcher: Dispatcher<PokerRoom>, /* ... */ }

impl Dispatchable for PokerRoom {
    fn dispatcher_mut(&mut self) -> &mut Dispatcher<Self> { &mut self.dispatcher }
}

// then, from any handler:
//   self.dispatch(ctx, DealCards { count: 2 }).await;
// and in on_dispose:
//   self.dispatcher_mut().stop();
```

Notes:
- Payloads are just struct fields (type-safe — no separate payload object).
- Inside `execute`, chain via the passed `dispatcher`, never `room.dispatch`.
- Commands run sequentially on the room actor, exactly like handlers.
- Unit-test commands without a server: `RoomContext::default()` +
  `room.dispatch(&mut ctx, MyCommand).await` (see `demos/trivia/server`).

## Background tasks: `RoomSender`

Long-running work (LLM calls, HTTP, DB, Redis subscriptions) must not block
the room actor. Spawn a task, then re-enter the room with a command-style
closure via `ctx.sender()`:

```rust
let sender = ctx.sender();
let llm = self.llm.clone();
tokio::spawn(async move {
    let question = llm.generate("hard", "history").await;
    sender.send(move |room: &mut MyRoom, ctx| Box::pin(async move {
        room.set_question(ctx, question); // runs on the room actor, in order
    }));
});
```

## Admin panel (`@colyseus/monitor` counterpart)

```rust
Server::new().admin_panel(Some("secret-token".to_string())) // token optional
```

Opens a self-contained React monitoring UI at `http://<host>:<port>/admin`
(built once via Vite and embedded in the binary — no separate service;
source: `crates/colyseus/admin-ui`, regenerate with `npm run build` there):

- **Overview**: uptime, RSS memory, room/connection counts, live room table
  (clients, max, lock state, metadata, filter fields, age) refreshing 2.5s
- **Room inspect**: full synchronized state (JSON), client list with
  connection state + auth info, reserved seats, pending reconnections
- **Live events**: every room streams its decoded traffic (joins, leaves,
  messages in/out, broadcasts, state patches with their JSON-Patch ops)
  over `GET /admin/api/rooms/{id}/events` (WebSocket)
- **Actions**: lock/unlock, kick client, send a message to one client or
  broadcast, dispose the room

The JSON API (`/admin/api/*`, bearer-token guarded when set):
`GET overview` · `GET rooms/{id}` · `POST rooms/{id}/lock|unlock|kick|message|dispose`.
Unlike the Colyseus monitor, arbitrary state *editing* is intentionally not
offered: state is a typed Rust struct, not a dynamic schema — mutate it via
commands instead.

## Demos

- [`demos/tictactoe`](demos/tictactoe) — tic-tac-toe + chat: T3 stack
  (Next.js/tRPC/NextAuth/Drizzle) frontend, JWT auth into `on_auth`,
  reconnection.
- [`demos/trivia`](demos/trivia) — LLM trivia arena (2–4 players + 10
  spectators, ready/start lobby flow, 10 rounds, global lobby chat,
  Redis-fed leaderboard). The backend is organized with the command pattern
  (`demos/trivia/server/src/commands/`) and is a good reference for
  structuring a real game.
- [`clients/ts/examples/smoke.mjs`](clients/ts/examples/smoke.mjs) — headless
  client smoke test; [`demos/trivia/sim.mjs`](demos/trivia/sim.mjs) plays a
  full 10-round game with two bot players.

## Benchmarks

Two examples form a measurable load harness — an isolated server and a client
spawner (both ends are real WebSocket + HTTP matchmaking traffic):

```sh
cargo build --release --examples

# terminal 1: the server under test
./target/release/examples/bench_server 127.0.0.1:2568

# terminal 2: <clients> <room_size> <msgs_per_sec_per_client> <duration_s> [state]
BENCH_CLIENT_ONLY=127.0.0.1:2568 ./target/release/examples/bench 3000 50 1 25
BENCH_CLIENT_ONLY=127.0.0.1:2568 ./target/release/examples/bench 1000 25 0 25 state
```

Watch the server with `ps -o rss,%cpu -p <pid>`.

Measured (single M-series core ≈ 1; derate ~3x for a cloud vCPU):

| Scenario | Result |
| --- | --- |
| Idle server | 7 MB RSS |
| Memory per connection (default WS buffers) | ~140 KB |
| Memory per connection (`ws_buffer_sizes(16KB, 32KB)`) | **~27 KB** |
| Broadcast fanout | ~100–150k msgs/s out per core |
| State sync, 40 rooms × 25 players, 30 Hz sim + 20 Hz patches | ~0.3 core (~3.3k players/core) |
| 6000 mostly-idle connections | ~165 MB RSS, ~1% CPU |

**Rough capacity for a 1 vCPU / 1 GB box** (with tuned buffers):

- 20–25 player state-synced rooms @ 20 Hz: **~800–1200 players** (CPU-bound;
  halving the patch rate nearly doubles it)
- Chat-style rooms of 50 with heavy chatter: **~300–500 players** (fanout-bound:
  `out_msgs = players × rate × room_size`); rooms of 10: ~1500
- Mostly-idle / lobby presence: **~10–20k connections** (memory-bound)

Tuning knobs: `Server::ws_buffer_sizes()` (5× memory per connection),
`patch_rate`, room size (fanout), `set_view_filter` for interest management,
and the usual `ulimit -n` / `somaxconn` for >1k connections.

## TypeScript client

[`clients/ts`](clients/ts) contains a ~250-line client
(`npm i @msgpack/msgpack` is its only dependency):

```ts
const client = new Client("http://localhost:2567");
const room = await client.joinOrCreate("game", { mode: "ranked" });
room.onStateChange((s) => console.log(s));
room.send("move", { vx: 1, vy: 0 });

room.onLeave(async () => {
  const rejoined = await client.reconnect(room); // uses the stored token
});
```

For mobile (Swift/Kotlin/Unity): implement the ~6 frame types above with any
msgpack library plus a JSON-Patch library (or a tiny applier — see
`clients/ts/src/client.ts`).

## Extending

- **Presence**: implement the `Presence` trait (pub/sub + KV) on top of Redis
  to share data across processes; pass it via `Server::presence(...)`.
- **Custom HTTP routes**: `Server::routes(router)` merges your axum router.
- **Lobby**: subscribe to `MatchMaker::subscribe()` for
  `RoomCreated/RoomUpdated/RoomRemoved` events and push them to a room of
  your own.

## Running the tests

```sh
cargo test            # unit + full end-to-end (real HTTP + WS round-trips)
```

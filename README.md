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
| Operator matchmaking filters (`clients.lt=2`, …) + custom match hooks | — | ✅ (`filter` in the matchmake body, `match_filter_fn` / `match_sort_fn`) |
| Strict `filter_by` semantics (no wildcard cross-joins) | — | ✅ (`strict_filter_fields`) |
| Idempotent room creation | — | ✅ (`unique_by`, `Idempotency-Key` header) |
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
| Admin SDK / custom RPCs | (ad-hoc / Cloud) | ✅ `admin_token` + `admin_rpc` + TS `AdminClient` |
| Snapshot persistence (public + internal state, resume after restart) | 🔜 | ✅ |
| `devMode` room restore | ✅ | 🔜 |

## Architecture

```
┌──────────────────────────── Server (axum) ───────────────────────────┐
│ POST /matchmake/{method}/{roomName} ──► MatchMaker                    │
│ GET  /rooms[/{name}]                 ──► Driver (listings)            │
│ GET  /ws/{roomId}?sessionId=…        ──► Room actor (via mailbox)     │
└───────────────────────────────────────────────────────────────────────┘
        MatchMaker ── owns ──► RoomHandle { room_id, mpsc::Sender }
        Driver     ── RoomListing { roomId, name, clients, locked, … }
                      (trait; `LocalDriver` in-memory default — the
                      multi-process seam, like `Presence`)
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

## Matchmaking semantics

`POST /matchmake/{method}/{roomName}` accepts either a bare options object or
the extended form `{ "options": {…}, "filter": {…} }` (detected by the
reserved keys). `filter` uses the same operator model as the room-query API
(`eq/ne/gt/gte/lt/lte/in/exists`) on core fields, the room type's `filter_by`
fields and `metadata.*`, and applies to `joinOrCreate` / `join`:

```json
{ "options": {}, "filter": { "clients": { "lt": 2 } } }
```

So `joinOrCreate("tictactoe", {}, { clients < 2 })` finds or creates a room
with a free seat atomically — no find-then-create race. An optional
`Idempotency-Key` header makes mutating calls safe to retry: the resulting
seat reservation is cached for 30s and replayed for duplicate requests
(scoped per `{method, roomName, key}`).

Room-type knobs (chained onto `Server::define`):

| knob | meaning |
| --- | --- |
| `filter_by(&["mode"])` | option fields rooms are matched/listed by |
| `sort_by(&[("clients", 1)])` | candidate ordering at match time |
| `unique_by(&["slug"])` | `create_room` reuses a live room with the same key (`CreateRoomOutcome.created == false`) instead of duplicating |
| `strict_filter_fields(true)` | a request missing a `filter_by` field only matches rooms also missing it (default `false` = Colyseus wildcard semantics) |
| `match_filter_fn(f)` / `match_sort_fn(f)` | custom predicate / comparator run at match time |
| `internal()` / `persistent(false)` | server-side-only creation / opt out of snapshots |

Server-side bootstrap (create global rooms, subscribe to lobby events) hooks
into `Server::listen` via `Server::on_start(|mm| async { … })` — it runs after
snapshot restore, before any connection is accepted.

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

## Persistence (snapshots)

Rooms can persist their full serializable footprint — the public state
(client-visible), a server-only *internal* state, and framework fields
(metadata, lock, seats, reconnections) — to a local snapshot store and resume
seamlessly across restarts.

```rust
use colyseus::{FileSnapshotStore, PersistenceConfig};

Server::new().persistence(
    PersistenceConfig::new(FileSnapshotStore::new("./snapshots"))
        .auto_save_interval(Duration::from_secs(1)) // debounce (default 1s)
        .save_on_dispose(true)   // final write on dispose (default)
        .delete_on_dispose(false), // keep the snapshot (default)
);
```

Three tiers of room data:

| Tier | Example | API |
| --- | --- | --- |
| Public state (synced) | board, scores, phase | `ctx.set_state` / `ctx.state_mut` |
| Internal state (private, serialized) | correct answer, password, pending queue | `ctx.set_internal` / `ctx.internal_mut` |
| Transient (rebuilt) | `Arc<LlmClient>`, timer ids, dispatcher | plain room fields |

Implement `on_restore` to resume. `on_create` always runs first (re-register
message handlers + defaults); then `on_restore` overlays the persisted state:

```rust
use colyseus::RoomSnapshot;

async fn on_restore(&mut self, ctx: &mut RoomContext, snapshot: &RoomSnapshot) -> Result<()> {
    ctx.restore_state::<GameState>(snapshot)?;
    ctx.restore_internal::<GameInternal>(snapshot)?;
    // rebuild services + re-arm timers from wall-clock deadlines
    self.rearm_timers(ctx).await;
    Ok(())
}
```

Notes:

- Snapshots are written atomically (temp file + fsync + rename) and checksummed;
  corrupt files are quarantined and the room restarts fresh.
- One snapshot file per room: `./snapshots/{roomId}.snap`.
- On `Server::listen`, persisted rooms are restored **before** traffic is
  accepted (`restore_all()` — also callable manually when serving via
  `Server::build()`).
- Timers (`set_timeout`) don't survive restarts. Persist wall-clock deadlines
  in state/internal and re-arm the timers in `on_restore`.
- Reconnection tokens and reserved seats are persisted too, so a client with
  a live reconnection token can rejoin after a restart.
- Bump `schema_version()` and implement `on_migrate(from, &mut snapshot)` when
  the persisted state shape changes.

See `demos/trivia` (internal state + re-armed timers) and `demos/tictactoe`
(public state only) for working examples, and
`tests/integration.rs::persistence_restores_state_and_internal_across_restart`
for the end-to-end restart test.

## Admin panel & admin SDK

### Panel (`@colyseus/monitor` counterpart)

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
  broadcast, dispose the room, edit state values in the JSON tree

The JSON API (`/admin/api/*`, bearer-token guarded when set):
`GET overview` · `GET schema` (capability catalog: registered room types with
their matchmaking knobs, admin RPC names + Rust type names, core filterable
listing fields — SDKs can discover instead of hardcoding) · `GET rooms`
(filtered, paged — see below) · `GET rooms/stats` · `GET rooms/{id}` ·
`POST rooms/{id}/lock|unlock|kick|message|dispose|state` ·
`GET rooms/{id}/events` (WebSocket traffic stream; token via
`Sec-WebSocket-Protocol: bearer.<token>`, echoed back on success — the
`?token=` query param remains as a deprecated fallback).
State edits are validated by a serialize→edit→deserialize round-trip, so type
mismatches (e.g. a string into an `i64` field) are rejected server-side.

**Filtered room queries.** `GET /admin/api/rooms` (and the public
`GET /rooms/{name}`) accept operator filters, sorting and pagination:

```text
name=tictactoe&clients=1            equality (waiting rooms)
clients.gte=1&clients.lt=4          ranges (gt/gte/lt/lte)
slug=abc123&mode.in=ranked,casual   any filter_by field; in
locked.exists=false                 presence (exists/notExists)
sort=createdAt:desc,clients:asc     whitelisted sort keys
limit=25&offset=0&count=true        paging; count=true returns only `total`
```

Responses are `{ items, total, limit, offset, nextOffset }`. Filter/sort fields
are validated against the room type's `filter_by` whitelist (plus core fields
and `metadata.*`); unknown fields → `521`, and `roomId` is always rejected
(it collides with the engine's generated id in the flattened listing).

### Admin SDK + custom RPCs (for your own backend)

Register typed, token-guarded RPCs server-side and call them from your
TypeScript backend. Enable the API (optionally without serving the panel UI)
and register RPCs:

```rust
use colyseus::{AdminContext, AdminRpc, Result, Server};
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ResetRoom { room_id: String }

#[derive(Serialize)]
struct ResetRoomResult { ok: bool }

#[async_trait]
impl AdminRpc for ResetRoom {
    type Params = ResetRoom;
    type Response = ResetRoomResult;
    async fn call(p: Self::Params, ctx: AdminContext) -> Result<Self::Response> {
        ctx.dispose_room(&p.room_id);
        Ok(ResetRoomResult { ok: true })
    }
}

let mut server = Server::new();
server.define("game", || GameRoom);
let server = server
    .admin_token(Some("backend-secret".to_string())) // API only, no /admin UI
    .admin_rpc::<ResetRoom>("resetRoom");
```

Each RPC is `POST /admin/api/rpc/{name}` with a `Bearer` token. `Params` is
deserialized from the JSON body; `Response` is serialized as the JSON reply.
Errors return `{ code, error }` with an HTTP status (invalid params → `400`).

Send an `Idempotency-Key: <key>` header to make a call retriable: a successful
response is cached for ~30s and replayed for duplicate keys (errors are never
cached). Handlers must be side-effect-safe under replay within that window —
a retried call returns the original response without re-running the handler.

`AdminContext` is the safe handle handed to every RPC:

| method | purpose |
| --- | --- |
| `list_rooms(name?)` / `room(id)` | query listings |
| `create_room(name, options)` | server-side room creation (returns `CreateRoomOutcome`; reuses a live room when `unique_by` matches) |
| `inspect_room(id)` | state + clients + seats |
| `dispose_room(id)` / `lock_room(id)` / `unlock_room(id)` | lifecycle |
| `kick(id, session_id)` | force-disconnect |
| `send_message(id, session_id?, type, payload)` | message one client or broadcast |
| `edit_state` / `set_state_path` / `remove_state_path` | validated state edits |
| `command_room::<MyRoom>(id, f)` | inject a typed closure into a room actor (`&mut MyRoom`) |
| `call_room::<MyRoom, Rpc>(id, params)` | run a room RPC from another RPC (request/response) |
| `presence()` / `subscribe()` | pub/sub + KV, lobby events |

> Conventions: multi-word RPC fields use `camelCase` on the wire (add
> `#[serde(rename_all = "camelCase")]`) to match the rest of the framework
> and the TypeScript SDK. For game-specific mutations that need the room's
> typed state, prefer [`AdminContext::command_room`](crates/colyseus/src/admin_rpc.rs)
> over raw state edits.

### Room-based RPCs (run on the room actor)

When the logic needs `&mut MyRoom` + `&mut RoomContext` (and should run
sequentially with the room's own handlers), register a **room RPC** — it runs
inside the room actor and returns a typed response:

```rust
use colyseus::{Room, RoomContext, RoomRpc, Result};
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct AdjustScore { player: String, delta: i64 }

#[derive(Serialize, Deserialize)]
struct ScoreResult { points: i64 }

#[async_trait]
impl RoomRpc<GameRoom> for AdjustScore {
    type Params = AdjustScore;
    type Response = ScoreResult;
    async fn call(room: &mut GameRoom, ctx: &mut RoomContext, p: AdjustScore) -> Result<ScoreResult> {
        let state = ctx.state_mut::<GameState>().unwrap();
        state.scores[p.player].points += p.delta; // typed, lock-free (actor)
        Ok(ScoreResult { points: state.scores[p.player].points })
    }
}

server.room_rpc::<GameRoom, AdjustScore>("adjustScore");
```

Wire: `POST /admin/api/rooms/{roomId}/rpc/{name}` (body = `Params`). The room id
is in the path, the RPC name is registered per room type, and a type mismatch
(room is not a `GameRoom`) returns an error. You can also invoke a room RPC from
an admin RPC via `ctx.call_room::<GameRoom, AdjustScore>(&room_id, params).await`.

TypeScript side ([`clients/ts/src/admin.ts`](clients/ts/src/admin.ts), no deps):

```ts
import { AdminClient } from "colyseus-rs-client/admin";

const admin = new AdminClient({
  baseUrl: "http://localhost:2567",
  token: "backend-secret",
});

await admin.listRooms();
await admin.kick(roomId, sessionId);
await admin.sendMessage(roomId, "system", { text: "hi" });
await admin.setStatePath(roomId, "/players/1/score", 100);

// custom RPCs, compile-time checked against a declared catalog
interface MyRpcs {
  resetRoom: { params: { roomId: string }; response: { ok: boolean } };
}
const typed = new AdminClient<MyRpcs>({ baseUrl: "http://localhost:2567", token: "backend-secret" });
const { ok } = await typed.call("resetRoom", { roomId });
await typed.callUntyped("notInTheCatalog", { anything: 1 }); // escape hatch

// room-based RPCs (run on the room actor)
const score = await admin.callRoom<{ points: number }>(roomId, "adjustScore", {
  player: "p1",
  delta: 10,
});

// capability discovery (room types, RPCs, filterable fields)
const schema = await admin.schema();

// live traffic stream (token travels in the WS subprotocol, not the URL)
const close = admin.roomEvents(roomId, (e) => console.log(e.kind));
```

Runnable: `cargo run --example admin_rpc` then
`node clients/ts/examples/admin-smoke.mjs`.

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

[`clients/ts`](clients/ts) contains a small client
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

Matchmaking calls accept per-call options — a server-side room `filter`
(`joinOrCreate` / `join`), a `timeout`, an abort `signal`, and an
`idempotencyKey` (auto-generated UUID by default; the server replays the same
seat reservation for a duplicate key within 30s, so retried/double-submitted
joins can't create ghost rooms):

```ts
const room = await client.joinOrCreate("tictactoe", {}, {
  filter: { clients: { lt: 2 } },   // only rooms with a free seat
  timeout: 5_000,
});

// server-side listing queries (GET /rooms) via the query builder
const page = await client.rooms((q) =>
  q.name("trivia").where("clients", "lt", 4).sort("createdAt", "desc").limit(20),
);
// → RoomQueryResult { items, total, limit, offset, nextOffset }
```

For mobile (Swift/Kotlin/Unity): implement the ~6 frame types above with any
msgpack library plus a JSON-Patch library (or a tiny applier — see
`clients/ts/src/client.ts`).

## Extending

- **Presence**: implement the `Presence` trait (pub/sub + KV) on top of Redis
  to share data across processes; pass it via `Server::presence(...)`.
- **Driver**: implement the `Driver` trait (room listings + matchmaking
  queries) on top of Redis/SQL for multi-process deployments; pass it via
  `Server::driver(...)`. Default: in-memory `LocalDriver`.
- **Custom HTTP routes**: `Server::routes(router)` merges your axum router.
- **Lobby**: subscribe to `MatchMaker::subscribe()` for
  `RoomCreated/RoomUpdated/RoomRemoved` events and push them to a room of
  your own.

## Running the tests

```sh
cargo test            # unit + full end-to-end (real HTTP + WS round-trips)
```

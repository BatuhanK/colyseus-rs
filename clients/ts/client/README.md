# colyseus-rs-client

TypeScript client for [colyseus-rs](https://github.com/BatuhanK/colyseus-rs) game servers: HTTP matchmaking, binary MessagePack frames over WebSocket, and room state sync via JSON-Patch (RFC 6902).

## Install

```sh
npm install colyseus-rs-client
```

## Usage

```ts
import { Client } from "colyseus-rs-client";

const client = new Client("http://localhost:2567");
const room = await client.joinOrCreate("game", { mode: "ranked" });

room.onStateChange((state) => render(state));
room.onMessage("chat", (msg) => console.log(msg));
room.send("move", { vx: 1, vy: 0 });

// reconnection: keep the token and call client.reconnect(room)
```

Public room listings (server-side filtered/paged query):

```ts
const page = await client.rooms((q) =>
  q.name("trivia").where("clients", "lt", 2).sort("createdAt", "desc").limit(20),
);
```

For the server administration API (room inspect/dispose, custom admin RPCs), see [`colyseus-rs-admin`](https://www.npmjs.com/package/colyseus-rs-admin).

## License

MIT

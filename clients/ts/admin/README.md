# colyseus-rs-admin

Admin SDK for [colyseus-rs](https://github.com/BatuhanK/colyseus-rs) game servers. Calls the token-guarded `/admin/api/*` HTTP API: filtered room queries, inspect/dispose/kick, and custom RPCs registered server-side with `Server::admin_rpc`.

## Install

```sh
npm install colyseus-rs-admin
```

## Usage

```ts
import { AdminClient } from "colyseus-rs-admin";

const admin = new AdminClient({ baseUrl: "http://localhost:2567", token: "backend-secret" });

const rooms = await admin.listRooms({ name: "game", sort: "createdAt:desc", limit: 10 });
await admin.kick("roomId", "sessionId");

// typed custom RPCs: declare the catalog once, get compile-time checking
interface MyRpcs {
  resetRoom: { params: { roomId: string }; response: { ok: boolean } };
}
const typed = new AdminClient<MyRpcs>({ baseUrl: "http://localhost:2567", token: "backend-secret" });
const res = await typed.call("resetRoom", { roomId: "r" }); // res: { ok: boolean }
await typed.callUntyped("anythingGoes", { x: 1 });          // escape hatch
```

For players joining rooms from the browser, see [`colyseus-rs-client`](https://www.npmjs.com/package/colyseus-rs-client).

## License

MIT

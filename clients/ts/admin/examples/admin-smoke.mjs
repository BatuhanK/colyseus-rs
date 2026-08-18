// Smoke test for the TypeScript admin SDK.
//
// Run the server first:
//   cargo run --example admin_rpc
// Then:
//   node examples/admin-smoke.mjs

import { AdminClient } from "../src/admin.ts";

const admin = new AdminClient({ baseUrl: "http://localhost:2567", token: "backend-secret" });

// filtered room query: empty before we create anything
const page = await admin.listRooms({ name: "game", sort: "createdAt:desc", limit: 10 });
console.log("rooms before:", page.total, JSON.stringify(page));

// capability discovery
const schema = await admin.schema();
console.log("schema:", schema.roomTypes.map((t) => t.name).join(", "), "| rpcs:", schema.adminRpcs.map((r) => r.name).join(", "));

// custom RPC: create a room
const created = await admin.callUntyped("createGame", { mode: "ranked" });
console.log("created room:", created.roomId);

// filtered query now sees it; operators work (clients=0)
const waiting = await admin.listRooms({ name: "game", filter: { clients: 0 }, count: true });
console.log("empty rooms (count):", waiting.total);
const viaId = await admin.listRooms({ name: "game", filter: { clients: { lte: 0 } }, limit: 1 });
console.log("first empty room:", viaId.items[0]?.roomId);

// room stats
console.log("stats:", JSON.stringify(await admin.roomStats("game")));

// findWaitingRoom helper (no waiting room while clients=0)
console.log("findWaitingRoom:", (await admin.findWaitingRoom("game", { clients: 1 })) ?? "none");

// custom RPC: broadcast into it
const shout = await admin.callUntyped("shout", { roomId: created.roomId, text: "hello from admin sdk" });
console.log("shout delivered:", shout.delivered);

// custom RPC: typed room access (reset score)
const reset = await admin.callUntyped("resetScore", { roomId: created.roomId });
console.log("reset found:", reset.found);

// room-based RPC: runs on the room actor, returns a response
const score = await admin.callRoom(created.roomId, "getScore", { player: "p1" });
console.log("getScore:", JSON.stringify(score));

// built-in: inspect the room
const detail = await admin.inspectRoom(created.roomId);
console.log("room name:", detail.listing.name, "| state:", JSON.stringify(detail.state));

// built-in: dispose it
await admin.disposeRoom(created.roomId);
console.log("disposed");

console.log("rooms after:", (await admin.listRooms({ count: true })).total);
process.exit(0);

// Smoke test for the TypeScript admin SDK.
//
// Run the server first:
//   cargo run --example admin_rpc
// Then:
//   node examples/admin-smoke.mjs

import { AdminClient } from "../src/admin.ts";

const admin = new AdminClient("http://localhost:2567", "backend-secret");

// built-in: list rooms
const rooms = await admin.listRooms();
console.log("rooms before:", rooms.length);

// custom RPC: create a room
const created = await admin.call("createGame", { mode: "ranked" });
console.log("created room:", created.roomId);

// custom RPC: broadcast into it
const shout = await admin.call("shout", { roomId: created.roomId, text: "hello from admin sdk" });
console.log("shout delivered:", shout.delivered);

// custom RPC: typed room access (reset score)
const reset = await admin.call("resetScore", { roomId: created.roomId });
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

console.log("rooms after:", (await admin.listRooms()).length);
process.exit(0);

import { Client } from "./src/client.ts";

const client = new Client("http://localhost:2567");
const room = await client.joinOrCreate("game", { mode: "ranked" });
console.log("joined room:", room.reservation.room.roomId, "serializer:", room.serializerId);

let stateChanges = 0;
room.onStateChange((s) => {
  stateChanges++;
});

room.send("move", { vx: 10, vy: 5 });
await new Promise((r) => setTimeout(r, 1000));
console.log("state changes after 1s:", stateChanges);
console.log("my player:", JSON.stringify(room.state.players[room.reservation.sessionId]));

// drop abnormally, then reconnect
const token = room.reconnectionToken;
room.close();
await new Promise((r) => setTimeout(r, 300));
const room2 = await client.reconnect(room);
await new Promise((r) => setTimeout(r, 200));
console.log("reconnected, same session:", room2.reservation.sessionId === room.reservation.sessionId);
console.log("state survived:", JSON.stringify(room2.state.players[room.reservation.sessionId]));
room2.leave();
process.exit(0);

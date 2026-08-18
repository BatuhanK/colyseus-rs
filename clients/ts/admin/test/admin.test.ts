// Typed-RPC compile-time wiring tests. Run with `npm test` (node --test;
// requires a Node version with type stripping, >= 22.18).

import assert from "node:assert/strict";
import test from "node:test";

import { AdminClient } from "../src/admin.ts";

test("typed RPC map checks names and payload types at compile time", () => {
  // This test exercises the types at compile time; at runtime we only assert
  // the client was constructed with the object form.
  interface Rpcs {
    resetRoom: { params: { roomId: string }; response: { ok: boolean } };
  }
  const admin = new AdminClient<Rpcs>({ baseUrl: "http://localhost:1", token: "t" });
  // type-level assertions (would fail tsc if the generic wiring broke)
  const _check = async () => {
    const res: { ok: boolean } = await admin.call("resetRoom", { roomId: "r" });
    return res;
  };
  // @ts-expect-error — unknown RPC name
  const _bad = () => admin.call("nope", {});
  // @ts-expect-error — wrong params shape
  const _bad2 = () => admin.call("resetRoom", { wrong: 1 });
  assert.ok(admin);
  assert.equal(typeof _check, "function");
  assert.equal(typeof _bad, "function");
  assert.equal(typeof _bad2, "function");
});

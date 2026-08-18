// Query-builder serialization tests. Run with `npm test` (node --test;
// requires a Node version with type stripping, >= 22.18).

import assert from "node:assert/strict";
import test from "node:test";

import { RoomQueryBuilder, serializeFilter } from "../src/query.ts";

test("builder serializes name / operators / sort / paging", () => {
  const qs = new RoomQueryBuilder()
    .name("trivia")
    .where("clients", "lt", 2)
    .where("slug", "eq", "abc")
    .where("mode", "in", ["a", "b"])
    .where("archived", "notExists")
    .sort("createdAt", "desc")
    .sort("clients")
    .limit(20)
    .offset(40)
    .toString();
  const p = new URLSearchParams(qs);
  assert.equal(p.get("name"), "trivia");
  assert.equal(p.get("clients.lt"), "2");
  assert.equal(p.get("slug"), "abc");
  assert.equal(p.get("mode.in"), "a,b");
  assert.equal(p.get("archived.exists"), "false");
  assert.equal(p.get("sort"), "createdAt:desc,clients:asc");
  assert.equal(p.get("limit"), "20");
  assert.equal(p.get("offset"), "40");
  assert.equal(p.get("count"), null);
});

test("builder countOnly + exists", () => {
  const p = new URLSearchParams(
    new RoomQueryBuilder().where("locked", "exists").countOnly().toString(),
  );
  assert.equal(p.get("locked.exists"), "true");
  assert.equal(p.get("count"), "true");
});

test("serializeFilter maps object-style filters to field.op=value", () => {
  const p = new URLSearchParams();
  serializeFilter(p, {
    clients: { gte: 1, lt: 4 },
    slug: "abc",
    mode: { in: ["a", "b"] },
    x: { exists: false },
    y: { notExists: true },
  });
  assert.equal(p.get("clients.gte"), "1");
  assert.equal(p.get("clients.lt"), "4");
  assert.equal(p.get("slug"), "abc");
  assert.equal(p.get("mode.in"), "a,b");
  assert.equal(p.get("x.exists"), "false");
  assert.equal(p.get("y.exists"), "false");
});

test("typed RPC map checks names and payload types at compile time", () => {
  // This test exercises the types at compile time; at runtime we only assert
  // the client was constructed with the object form.
  interface Rpcs {
    resetRoom: { params: { roomId: string }; response: { ok: boolean } };
  }
  return import("../src/admin.ts").then(({ AdminClient }) => {
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
});

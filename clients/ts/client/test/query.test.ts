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

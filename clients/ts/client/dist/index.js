// src/query.ts
var RoomQueryBuilder = class {
  params = new URLSearchParams();
  sortKeys = [];
  /** Restrict to a single room type name. */
  name(name) {
    this.params.set("name", name);
    return this;
  }
  where(field, op, value) {
    switch (op) {
      case "eq":
        this.params.set(field, String(value));
        break;
      case "in":
        this.params.set(`${field}.in`, value.join(","));
        break;
      case "exists":
        this.params.set(`${field}.exists`, "true");
        break;
      case "notExists":
        this.params.set(`${field}.exists`, "false");
        break;
      default:
        this.params.set(`${field}.${op}`, String(value));
    }
    return this;
  }
  /** Equality shorthand: `whereEq("slug", "abc")` → `slug=abc`. */
  whereEq(field, value) {
    return this.where(field, "eq", value);
  }
  /** Add a sort key (repeatable; earlier keys win). */
  sort(field, direction = "asc") {
    this.sortKeys.push(`${field}:${direction}`);
    return this;
  }
  limit(limit) {
    this.params.set("limit", String(limit));
    return this;
  }
  offset(offset) {
    this.params.set("offset", String(offset));
    return this;
  }
  /** Only compute `total` (items come back empty). */
  countOnly() {
    this.params.set("count", "true");
    return this;
  }
  /** The serialized query string, without the leading `?`. */
  toString() {
    if (this.sortKeys.length > 0) this.params.set("sort", this.sortKeys.join(","));
    return this.params.toString();
  }
};
function serializeFilter(params, filter) {
  for (const [field, value] of Object.entries(filter)) {
    if (value === void 0 || value === null) continue;
    if (typeof value === "object") {
      for (const [op, v] of Object.entries(value)) {
        if (v === void 0) continue;
        if (op === "in") params.set(`${field}.in`, Array.isArray(v) ? v.join(",") : String(v));
        else if (op === "notExists") params.set(`${field}.exists`, String(!v));
        else params.set(`${field}.${op}`, String(v));
      }
    } else {
      params.set(field, String(value));
    }
  }
}

// src/util.ts
function combineAbortSignals(timeoutMs, signal) {
  if (timeoutMs === void 0 && signal === void 0) {
    return { signal: void 0, cancel: () => {
    } };
  }
  const controller = new AbortController();
  const timer = timeoutMs !== void 0 ? setTimeout(() => controller.abort(new Error(`request timed out after ${timeoutMs}ms`)), timeoutMs) : void 0;
  const onAbort = () => controller.abort(signal.reason);
  if (signal) {
    if (signal.aborted) controller.abort(signal.reason);
    else signal.addEventListener("abort", onAbort, { once: true });
  }
  return {
    signal: controller.signal,
    cancel: () => {
      if (timer !== void 0) clearTimeout(timer);
      signal?.removeEventListener("abort", onAbort);
    }
  };
}
function randomIdempotencyKey() {
  const c = globalThis.crypto;
  if (typeof c?.randomUUID === "function") return c.randomUUID();
  const bytes = new Uint8Array(16);
  if (typeof c?.getRandomValues === "function") {
    c.getRandomValues(bytes);
  } else {
    for (let i = 0; i < 16; i++) bytes[i] = Math.floor(Math.random() * 256);
  }
  bytes[6] = bytes[6] & 15 | 64;
  bytes[8] = bytes[8] & 63 | 128;
  const hex = [...bytes].map((b) => b.toString(16).padStart(2, "0")).join("");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

// src/client.ts
import { encode, decode } from "@msgpack/msgpack";
var JOIN_ROOM = 10;
var ERROR = 11;
var LEAVE_ROOM = 12;
var ROOM_DATA = 13;
var ROOM_STATE = 14;
var ROOM_STATE_PATCH = 15;
var ROOM_DATA_BYTES = 17;
var PING = 18;
var MatchmakeError = class extends Error {
  code;
  constructor(code, message) {
    super(message);
    this.code = code;
  }
};
var Room = class {
  state = void 0;
  ws;
  messageHandlers = /* @__PURE__ */ new Map();
  stateHandlers = [];
  leaveHandlers = [];
  errorHandlers = [];
  /** Token for reconnection, delivered in the join handshake. */
  reconnectionToken;
  /** "json-patch" when the room has synchronized state, "none" otherwise. */
  serializerId;
  baseUrl;
  reservation;
  constructor(baseUrl, reservation) {
    this.baseUrl = baseUrl;
    this.reservation = reservation;
  }
  /** @internal */
  connect(reconnectionToken) {
    const wsBase = this.baseUrl.replace(/^http/, "ws");
    let url = `${wsBase}/ws/${this.reservation.room.roomId}?sessionId=${this.reservation.sessionId}`;
    if (reconnectionToken) url += `&reconnectionToken=${encodeURIComponent(reconnectionToken)}`;
    this.ws = new WebSocket(url);
    this.ws.binaryType = "arraybuffer";
    return new Promise((resolve, reject) => {
      this.ws.onerror = () => reject(new Error("websocket connection failed"));
      this.ws.onclose = (ev) => {
        for (const h of this.leaveHandlers) h(ev.code);
      };
      this.ws.onmessage = (ev) => {
        const bytes = new Uint8Array(ev.data);
        if (bytes.length === 1 && bytes[0] === PING) return;
        const frame = decode(bytes);
        const code = frame[0];
        if (code === JOIN_ROOM) {
          this.reconnectionToken = frame[1];
          this.serializerId = frame[2];
          resolve();
        } else if (code === ERROR) {
          for (const h of this.errorHandlers) h(frame[1], frame[2]);
        } else if (code === ROOM_STATE) {
          this.state = frame[1];
          this.emitState();
        } else if (code === ROOM_STATE_PATCH) {
          if (this.state === void 0) this.state = {};
          applyPatch(this.state, frame[1]);
          this.emitState();
        } else if (code === ROOM_DATA || code === ROOM_DATA_BYTES) {
          const type = frame[1];
          for (const h of this.messageHandlers.get(type) ?? []) h(frame[2]);
          for (const h of this.messageHandlers.get("*") ?? []) h([type, frame[2]]);
        }
      };
    });
  }
  send(type, payload) {
    this.ws.send(encode([ROOM_DATA, type, payload ?? null]));
  }
  sendBytes(type, bytes) {
    this.ws.send(encode([ROOM_DATA_BYTES, type, bytes]));
  }
  ping() {
    this.ws.send(new Uint8Array([PING]));
  }
  leave() {
    try {
      this.ws.send(encode([LEAVE_ROOM]));
    } finally {
      this.ws.close();
    }
  }
  /** Close the socket without a consented LEAVE (triggers on_drop server-side). */
  close() {
    this.ws.close();
  }
  onMessage(type, handler) {
    const list = this.messageHandlers.get(type) ?? [];
    list.push(handler);
    this.messageHandlers.set(type, list);
  }
  onStateChange(handler) {
    this.stateHandlers.push(handler);
  }
  /**
   * Notify state handlers with a deep copy. The internal state object is
   * mutated in place by patches — handing it out directly would alias
   * previous snapshots (breaking change detection in React & friends).
   */
  emitState() {
    const snapshot = structuredClone(this.state);
    for (const h of this.stateHandlers) h(snapshot);
  }
  onLeave(handler) {
    this.leaveHandlers.push(handler);
  }
  onError(handler) {
    this.errorHandlers.push(handler);
  }
};
var Client = class {
  baseUrl;
  getHeaders;
  /**
   * @param baseUrl http(s) base url of the game server
   * @param getHeaders optional extra headers for matchmaking requests
   *        (e.g. `() => ({ authorization: \`Bearer \${token}\` })`)
   */
  constructor(baseUrl, getHeaders) {
    this.baseUrl = baseUrl;
    this.getHeaders = getHeaders;
  }
  joinOrCreate(roomName, options = {}, call = {}) {
    return this.matchmake("joinOrCreate", roomName, options, call);
  }
  create(roomName, options = {}, call = {}) {
    return this.matchmake("create", roomName, options, call);
  }
  join(roomName, options = {}, call = {}) {
    return this.matchmake("join", roomName, options, call);
  }
  joinById(roomId, options = {}, call = {}) {
    return this.matchmake("joinById", roomId, options, call);
  }
  /**
   * Query public room listings (GET /rooms), returning a page of results.
   *
   * ```ts
   * const page = await client.rooms((q) =>
   *   q.name("trivia").where("clients", "lt", 2).sort("createdAt", "desc").limit(20),
   * );
   * ```
   *
   * Passing a plain room name (`client.rooms("trivia")`) lists that type with
   * the server's default paging.
   */
  async rooms(query) {
    let url;
    if (typeof query === "function") {
      const q = new RoomQueryBuilder();
      query(q);
      const qs = q.toString();
      url = `${this.baseUrl}/rooms${qs ? `?${qs}` : ""}`;
    } else {
      url = `${this.baseUrl}/rooms${query ? `/${query}` : ""}`;
    }
    const res = await fetch(url);
    return res.json();
  }
  async matchmake(method, roomName, options, call) {
    const body = call.filter && (method === "joinOrCreate" || method === "join") ? { options: options ?? {}, filter: call.filter } : options ?? {};
    const headers = {
      "content-type": "application/json",
      ...this.getHeaders?.()
    };
    headers["idempotency-key"] = call.idempotencyKey ?? randomIdempotencyKey();
    const { signal, cancel } = combineAbortSignals(call.timeout, call.signal);
    let res;
    try {
      res = await fetch(`${this.baseUrl}/matchmake/${method}/${roomName}`, {
        method: "POST",
        headers,
        body: JSON.stringify(body),
        signal
      });
    } finally {
      cancel();
    }
    if (!res.ok) {
      const body2 = await res.json().catch(() => ({ code: res.status, error: res.statusText }));
      throw new MatchmakeError(body2.code, body2.error);
    }
    const reservation = await res.json();
    const room = new Room(this.baseUrl, reservation);
    await room.connect();
    return room;
  }
  /** Reconnect into a room after a drop (server must have called allow_reconnection). */
  async reconnect(room, call = {}) {
    if (!room.reconnectionToken) throw new Error("no reconnection token");
    return this.reconnectById(room.reservation.room.roomId, room.reconnectionToken, call);
  }
  /**
   * Reconnect with a raw roomId + token — e.g. after a page reload, where the
   * previous Room object no longer exists (persist the token in
   * sessionStorage if you want F5-proof sessions).
   */
  async reconnectById(roomId, reconnectionToken, call = {}) {
    const { signal, cancel } = combineAbortSignals(call.timeout, call.signal);
    let res;
    try {
      res = await fetch(`${this.baseUrl}/matchmake/reconnect/${roomId}`, {
        method: "POST",
        headers: { "content-type": "application/json", ...this.getHeaders?.() },
        body: JSON.stringify({ reconnectionToken }),
        signal
      });
    } finally {
      cancel();
    }
    if (!res.ok) {
      const body = await res.json().catch(() => ({ code: res.status, error: res.statusText }));
      throw new MatchmakeError(body.code, body.error);
    }
    const reservation = await res.json();
    const room = new Room(this.baseUrl, reservation);
    await room.connect(reconnectionToken);
    return room;
  }
};
function parsePath(path) {
  return path.split("/").slice(1).map((seg) => seg.replace(/~1/g, "/").replace(/~0/g, "~"));
}
function applyPatch(doc, patch) {
  for (const op of patch) {
    const segments = parsePath(op.path);
    let parent = doc;
    for (let i = 0; i < segments.length - 1; i++) {
      const key2 = Array.isArray(parent) ? Number(segments[i]) : segments[i];
      parent = parent[key2];
      if (parent === void 0 || parent === null) {
        throw new Error(`json-patch: invalid path ${op.path}`);
      }
    }
    const last = segments[segments.length - 1];
    const key = Array.isArray(parent) ? last === "-" ? parent.length : Number(last) : last;
    switch (op.op) {
      case "add":
      case "replace":
        if (Array.isArray(parent)) parent.splice(key, op.op === "add" ? 0 : 1, op.value);
        else parent[key] = op.value;
        break;
      case "remove":
        if (Array.isArray(parent)) parent.splice(key, 1);
        else delete parent[key];
        break;
      default:
        throw new Error(`json-patch: unsupported op ${op.op}`);
    }
  }
}
export {
  Client,
  MatchmakeError,
  Room,
  RoomQueryBuilder,
  combineAbortSignals,
  randomIdempotencyKey,
  serializeFilter
};

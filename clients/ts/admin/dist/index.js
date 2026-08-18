// src/admin.ts
import {
  combineAbortSignals,
  serializeFilter
} from "colyseus-rs-client";
import {
  RoomQueryBuilder
} from "colyseus-rs-client";
var AdminError = class extends Error {
  code;
  constructor(message, code) {
    super(message);
    this.name = "AdminError";
    this.code = code;
  }
};
var AdminClient = class {
  baseUrl;
  token;
  timeoutMs;
  fetchFn;
  constructor(baseUrlOrOptions, token, timeoutMs = 1e4) {
    const options = typeof baseUrlOrOptions === "string" ? { baseUrl: baseUrlOrOptions, token, timeout: timeoutMs } : baseUrlOrOptions;
    this.baseUrl = options.baseUrl.replace(/\/+$/, "");
    this.token = options.token;
    this.timeoutMs = options.timeout ?? 1e4;
    this.fetchFn = options.fetch ?? ((...args) => fetch(...args));
  }
  setToken(token) {
    this.token = token;
  }
  // ------------------------------------------------------------------
  // Built-in admin operations
  // ------------------------------------------------------------------
  /** Process stats + room listings. */
  async overview(call = {}) {
    return this.request("/admin/api/overview", { call });
  }
  // ------------------------------------------------------------------
  // Filtered room queries
  // ------------------------------------------------------------------
  /**
   * Query rooms server-side with filters, sorting and pagination
   * (GET /admin/api/rooms).
   *
   * ```ts
   * const page = await admin.listRooms({
   *   name: "tictactoe",
   *   filter: { clients: 1 },          // waiting for an opponent
   *   sort: "createdAt:desc",
   *   limit: 25,
   * });
   * ```
   */
  async listRooms(query, call = {}) {
    return this.request(`/admin/api/rooms${buildQueryString(query)}`, { call });
  }
  /** All rooms of a type (auto-paginated) — for dashboards / panels. */
  async listRoomsAll(name, call = {}) {
    const out = [];
    let offset = 0;
    for (; ; ) {
      const page = await this.listRooms({ name, limit: 500, offset }, call);
      out.push(...page.items);
      if (page.nextOffset === null) return out;
      offset = page.nextOffset;
    }
  }
  /** Per-room-type status counts (open / waiting / full / locked …). */
  async roomStats(name, call = {}) {
    return this.request(`/admin/api/rooms/stats${name ? `?name=${encodeURIComponent(name)}` : ""}`, { call });
  }
  /**
   * First room of type `name` matching `filter` — e.g. a room waiting for an
   * opponent: `findWaitingRoom("tictactoe", { clients: 1 })`.
   */
  async findWaitingRoom(name, filter, call = {}) {
    const page = await this.listRooms({ name, filter, limit: 1 }, call);
    return page.items[0];
  }
  /** Inspect a room: full state, clients, seats, reconnections. */
  async inspectRoom(roomId, call = {}) {
    return this.request(`/admin/api/rooms/${encodeURIComponent(roomId)}`, { call });
  }
  /** Lock a room against new seat reservations. */
  async lockRoom(roomId, call = {}) {
    await this.request(`/admin/api/rooms/${encodeURIComponent(roomId)}/lock`, { method: "POST", call });
  }
  /** Unlock a room. */
  async unlockRoom(roomId, call = {}) {
    await this.request(`/admin/api/rooms/${encodeURIComponent(roomId)}/unlock`, { method: "POST", call });
  }
  /** Force-disconnect a client. */
  async kick(roomId, sessionId, call = {}) {
    await this.request(`/admin/api/rooms/${encodeURIComponent(roomId)}/kick`, {
      method: "POST",
      body: { sessionId },
      call
    });
  }
  /**
   * Send a message to one client (`sessionId` set) or broadcast to all
   * (`sessionId` omitted). `type` is a string or number.
   */
  async sendMessage(roomId, type, data, sessionId, call = {}) {
    await this.request(`/admin/api/rooms/${encodeURIComponent(roomId)}/message`, {
      method: "POST",
      body: { sessionId: sessionId ?? null, type, data },
      call
    });
  }
  /** Dispose a room (all clients disconnected). */
  async disposeRoom(roomId, call = {}) {
    await this.request(`/admin/api/rooms/${encodeURIComponent(roomId)}/dispose`, { method: "POST", call });
  }
  /**
   * Edit room state at a JSON-pointer-ish path.
   * @param path e.g. "/players/abc123/score" (numeric segments index arrays)
   * @param op   "set" or "remove"
   */
  async editState(roomId, path, op, value, call = {}) {
    await this.request(`/admin/api/rooms/${encodeURIComponent(roomId)}/state`, {
      method: "POST",
      body: { path, op, value },
      call
    });
  }
  /** Set a value in room state at a path (see {@link editState}). */
  async setStatePath(roomId, path, value, call = {}) {
    await this.editState(roomId, path, "set", value, call);
  }
  /** Remove a value from room state at a path (see {@link editState}). */
  async removeStatePath(roomId, path, call = {}) {
    await this.editState(roomId, path, "remove", void 0, call);
  }
  // ------------------------------------------------------------------
  // Capability discovery
  // ------------------------------------------------------------------
  /**
   * Machine-readable catalog of the server's registered room types (with
   * their `filterBy` / `uniqueBy` / `sortBy` knobs), admin RPCs, and core
   * filterable listing fields (GET /admin/api/schema).
   */
  async schema(call = {}) {
    return this.request("/admin/api/schema", { call });
  }
  // ------------------------------------------------------------------
  // Custom RPCs
  // ------------------------------------------------------------------
  /**
   * Call a custom admin RPC registered server-side via `Server::admin_rpc`,
   * compile-time checked against the client's `RpcMap`:
   *
   * ```ts
   * const res = await admin.call("resetRoom", { roomId });
   * ```
   *
   * Pass an `idempotencyKey` in the call options to make retried mutating
   * RPCs safe (the server replays the first response for a duplicate key).
   * For RPCs not declared in the map, use {@link callUntyped}.
   */
  async call(name, params, call = {}) {
    return this.callUntyped(name, params, call);
  }
  /**
   * Untyped escape hatch for admin RPCs not declared in the `RpcMap`
   * (or when no map was provided).
   */
  async callUntyped(name, params, call = {}) {
    return this.request(`/admin/api/rpc/${encodeURIComponent(name)}`, {
      method: "POST",
      body: params ?? null,
      call
    });
  }
  /**
   * Call a room-based RPC registered server-side via `Server::room_rpc`. The
   * handler runs on the room actor with typed `&mut MyRoom` access and returns
   * its response.
   *
   * ```ts
   * const score = await admin.callRoom<{ player: string; points: number }>(roomId, "getScore", { player: "p1" });
   * ```
   */
  async callRoom(roomId, name, params, call = {}) {
    return this.request(
      `/admin/api/rooms/${encodeURIComponent(roomId)}/rpc/${encodeURIComponent(name)}`,
      { method: "POST", body: params ?? null, call }
    );
  }
  // ------------------------------------------------------------------
  // Live events stream
  // ------------------------------------------------------------------
  /**
   * Subscribe to a room's decoded traffic stream (joins, leaves, messages,
   * broadcasts, state patches). Returns a function that closes the socket.
   *
   * The token is sent as a WebSocket subprotocol
   * (`Sec-WebSocket-Protocol: bearer.<token>`, echoed by the server on
   * success) so it doesn't leak into access logs. Pass
   * `opts.legacyTokenParam` to use the deprecated `?token=` query param for
   * older servers.
   */
  roomEvents(roomId, onEvent, onClose, opts) {
    const wsBase = this.baseUrl.replace(/^http/, "ws");
    let url = `${wsBase}/admin/api/rooms/${encodeURIComponent(roomId)}/events`;
    let protocols;
    if (this.token) {
      if (opts?.legacyTokenParam) {
        url += `?token=${encodeURIComponent(this.token)}`;
      } else {
        protocols = [`bearer.${this.token}`];
      }
    }
    const ws = new WebSocket(url, protocols);
    ws.onmessage = (ev) => onEvent(JSON.parse(ev.data));
    ws.onclose = onClose ?? null;
    return () => ws.close();
  }
  // ------------------------------------------------------------------
  // Internal
  // ------------------------------------------------------------------
  async request(path, opts = {}) {
    const headers = { "content-type": "application/json" };
    if (this.token) headers.authorization = `Bearer ${this.token}`;
    if (opts.call?.idempotencyKey) headers["idempotency-key"] = opts.call.idempotencyKey;
    const { signal, cancel } = combineAbortSignals(
      opts.call?.timeout ?? this.timeoutMs,
      opts.call?.signal
    );
    try {
      const res = await this.fetchFn(`${this.baseUrl}${path}`, {
        method: opts.method ?? "GET",
        headers,
        body: opts.body !== void 0 ? JSON.stringify(opts.body) : void 0,
        signal
      });
      if (res.status === 401) throw new AdminError("unauthorized", 401);
      if (!res.ok) {
        const body = await res.json().catch(() => ({}));
        throw new AdminError(body.error ?? res.statusText, body.code ?? res.status);
      }
      if (res.status === 204) return null;
      return res.json();
    } finally {
      cancel();
    }
  }
};
function buildQueryString(query) {
  if (!query) return "";
  const params = new URLSearchParams();
  if (query.name) params.set("name", query.name);
  if (query.filter) serializeFilter(params, query.filter);
  if (query.sort) {
    const parts = Array.isArray(query.sort) ? query.sort : [query.sort];
    params.set(
      "sort",
      parts.map((s2) => s2.includes(":") ? s2 : `${s2}:asc`).join(",")
    );
  }
  if (query.limit !== void 0) params.set("limit", String(query.limit));
  if (query.offset !== void 0) params.set("offset", String(query.offset));
  if (query.count) params.set("count", "true");
  const s = params.toString();
  return s ? `?${s}` : "";
}
export {
  AdminClient,
  AdminError,
  RoomQueryBuilder
};

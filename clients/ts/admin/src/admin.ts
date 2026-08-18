/**
 * Admin SDK for colyseus-rs servers.
 *
 * Call built-in admin operations and custom RPCs (registered server-side with
 * `Server::admin_rpc`) over the token-guarded `/admin/api/*` HTTP API.
 *
 * ```ts
 * import { AdminClient } from "colyseus-rs-admin";
 *
 * const admin = new AdminClient({ baseUrl: "http://localhost:2567", token: "backend-secret" });
 *
 * const rooms = await admin.listRooms();
 * await admin.kick("roomId", "sessionId");
 *
 * // typed custom RPCs: declare the catalog once, get compile-time checking
 * interface MyRpcs {
 *   resetRoom: { params: { roomId: string }; response: { ok: boolean } };
 * }
 * const typed = new AdminClient<MyRpcs>({ baseUrl, token });
 * const res = await typed.call("resetRoom", { roomId }); // res: { ok: boolean }
 * await typed.callUntyped("anythingGoes", { x: 1 });      // escape hatch
 * ```
 *
 * Only dependency: `colyseus-rs-client` (shared room-query types/builder);
 * otherwise uses `fetch` and (for the events stream) `WebSocket`.
 */

import {
  combineAbortSignals,
  serializeFilter,
  type CallOptions,
  type RoomFilter,
  type RoomListing,
  type RoomQueryResult,
} from "colyseus-rs-client";

export {
  RoomQueryBuilder,
  type CallOptions,
  type RoomFilter,
  type RoomFilterValue,
  type RoomListing,
  type RoomQueryOp,
  type RoomQueryResult,
} from "colyseus-rs-client";

export interface Overview {
  processId: string;
  pid: number;
  uptimeMillis: number;
  rssBytes: number;
  rooms: number;
  connections: number;
}

/** Filtered, paged room query (server-side; GET /admin/api/rooms). */
export interface RoomQuery {
  /** Room type name. */
  name?: string;
  /** Field filters: `{ slug: "abc", clients: { gte: 1 } }`. */
  filter?: RoomFilter;
  /** Sort keys, e.g. `"createdAt:desc"` or `["createdAt:desc", "clients:asc"]`. */
  sort?: string | string[];
  limit?: number;
  offset?: number;
  /** Only compute `total` (items are empty). */
  count?: boolean;
}

/** Per-room-type status counts (GET /admin/api/rooms/stats). */
export interface RoomStats {
  total: number;
  open: number;
  waiting: number;
  full: number;
  locked: number;
  private: number;
}

export interface ClientInspection {
  sessionId: string;
  state: string;
  auth?: any;
  userData?: any;
}

export interface RoomInspection {
  roomId: string;
  state?: any;
  clients: ClientInspection[];
  reservedSeats: number;
  pendingReconnections: number;
  elapsedMillis: number;
  listing: RoomListing;
}

export interface RoomEventLog {
  at: number;
  direction: "in" | "out" | "sys";
  kind: string;
  client?: string;
  msgType?: string;
  payload?: any;
  bytes: number;
}

/** A registered room type, as reported by `GET /admin/api/schema`. */
export interface RoomTypeSchema {
  name: string;
  filterBy: string[];
  uniqueBy: string[];
  /** `[field, direction]` pairs — `1` asc, `-1` desc. */
  sortBy: [string, number][];
  strictFilterFields: boolean;
  internal: boolean;
  persistent: boolean;
  defaultOptions?: any;
}

/** A registered admin RPC, as reported by `GET /admin/api/schema`. */
export interface AdminRpcSchema {
  name: string;
  /** Rust params type name. */
  params: string;
  /** Rust response type name. */
  response: string;
}

/** Machine-readable capability catalog (GET /admin/api/schema). */
export interface AdminSchema {
  roomTypes: RoomTypeSchema[];
  adminRpcs: AdminRpcSchema[];
  coreFilterFields: string[];
}

/**
 * Typed admin RPC catalog: maps RPC name → `{ params, response }`.
 * Pass it as the `AdminClient` generic to make `call()` compile-time checked.
 */
export type AdminRpcCatalog = Record<string, { params: any; response: any }>;

export class AdminError extends Error {
  code?: number;
  constructor(message: string, code?: number) {
    super(message);
    this.name = "AdminError";
    this.code = code;
  }
}

export interface AdminClientOptions {
  /** http(s) base url of the game server. */
  baseUrl: string;
  /**
   * The admin bearer token (set with `Server::admin_token` /
   * `Server::admin_panel` on the server).
   */
  token?: string;
  /** Default per-request timeout in ms (aborts slow calls; default 10s). */
  timeout?: number;
  /** Custom `fetch` implementation (default: the global one). */
  fetch?: typeof fetch;
}

export class AdminClient<RpcMap extends AdminRpcCatalog = {}> {
  private baseUrl: string;
  private token?: string;
  private timeoutMs: number;
  private fetchFn: typeof fetch;

  /**
   * Legacy positional form — prefer `new AdminClient({ baseUrl, token, … })`.
   *
   * @param baseUrl   http(s) base url of the game server
   * @param token     the admin bearer token
   * @param timeoutMs default fetch timeout (default 10s)
   */
  constructor(baseUrl: string, token?: string, timeoutMs?: number);
  constructor(options: AdminClientOptions);
  constructor(
    baseUrlOrOptions: string | AdminClientOptions,
    token?: string,
    timeoutMs = 10_000,
  ) {
    const options: AdminClientOptions =
      typeof baseUrlOrOptions === "string"
        ? { baseUrl: baseUrlOrOptions, token, timeout: timeoutMs }
        : baseUrlOrOptions;
    this.baseUrl = options.baseUrl.replace(/\/+$/, "");
    this.token = options.token;
    this.timeoutMs = options.timeout ?? 10_000;
    this.fetchFn = options.fetch ?? ((...args) => fetch(...args));
  }

  setToken(token: string) {
    this.token = token;
  }

  // ------------------------------------------------------------------
  // Built-in admin operations
  // ------------------------------------------------------------------

  /** Process stats + room listings. */
  async overview(call: CallOptions = {}): Promise<Overview> {
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
  async listRooms(query?: RoomQuery, call: CallOptions = {}): Promise<RoomQueryResult> {
    return this.request(`/admin/api/rooms${buildQueryString(query)}`, { call });
  }

  /** All rooms of a type (auto-paginated) — for dashboards / panels. */
  async listRoomsAll(name?: string, call: CallOptions = {}): Promise<RoomListing[]> {
    const out: RoomListing[] = [];
    let offset = 0;
    for (;;) {
      const page = await this.listRooms({ name, limit: 500, offset }, call);
      out.push(...page.items);
      if (page.nextOffset === null) return out;
      offset = page.nextOffset;
    }
  }

  /** Per-room-type status counts (open / waiting / full / locked …). */
  async roomStats(name?: string, call: CallOptions = {}): Promise<RoomStats> {
    return this.request(`/admin/api/rooms/stats${name ? `?name=${encodeURIComponent(name)}` : ""}`, { call });
  }

  /**
   * First room of type `name` matching `filter` — e.g. a room waiting for an
   * opponent: `findWaitingRoom("tictactoe", { clients: 1 })`.
   */
  async findWaitingRoom(
    name: string,
    filter: RoomFilter,
    call: CallOptions = {},
  ): Promise<RoomListing | undefined> {
    const page = await this.listRooms({ name, filter, limit: 1 }, call);
    return page.items[0];
  }

  /** Inspect a room: full state, clients, seats, reconnections. */
  async inspectRoom(roomId: string, call: CallOptions = {}): Promise<RoomInspection> {
    return this.request(`/admin/api/rooms/${encodeURIComponent(roomId)}`, { call });
  }

  /** Lock a room against new seat reservations. */
  async lockRoom(roomId: string, call: CallOptions = {}): Promise<void> {
    await this.request(`/admin/api/rooms/${encodeURIComponent(roomId)}/lock`, { method: "POST", call });
  }

  /** Unlock a room. */
  async unlockRoom(roomId: string, call: CallOptions = {}): Promise<void> {
    await this.request(`/admin/api/rooms/${encodeURIComponent(roomId)}/unlock`, { method: "POST", call });
  }

  /** Force-disconnect a client. */
  async kick(roomId: string, sessionId: string, call: CallOptions = {}): Promise<void> {
    await this.request(`/admin/api/rooms/${encodeURIComponent(roomId)}/kick`, {
      method: "POST",
      body: { sessionId },
      call,
    });
  }

  /**
   * Send a message to one client (`sessionId` set) or broadcast to all
   * (`sessionId` omitted). `type` is a string or number.
   */
  async sendMessage(
    roomId: string,
    type: string | number,
    data: any,
    sessionId?: string,
    call: CallOptions = {},
  ): Promise<void> {
    await this.request(`/admin/api/rooms/${encodeURIComponent(roomId)}/message`, {
      method: "POST",
      body: { sessionId: sessionId ?? null, type, data },
      call,
    });
  }

  /** Dispose a room (all clients disconnected). */
  async disposeRoom(roomId: string, call: CallOptions = {}): Promise<void> {
    await this.request(`/admin/api/rooms/${encodeURIComponent(roomId)}/dispose`, { method: "POST", call });
  }

  /**
   * Edit room state at a JSON-pointer-ish path.
   * @param path e.g. "/players/abc123/score" (numeric segments index arrays)
   * @param op   "set" or "remove"
   */
  async editState(
    roomId: string,
    path: string,
    op: "set" | "remove",
    value?: any,
    call: CallOptions = {},
  ): Promise<void> {
    await this.request(`/admin/api/rooms/${encodeURIComponent(roomId)}/state`, {
      method: "POST",
      body: { path, op, value },
      call,
    });
  }

  /** Set a value in room state at a path (see {@link editState}). */
  async setStatePath(roomId: string, path: string, value: any, call: CallOptions = {}): Promise<void> {
    await this.editState(roomId, path, "set", value, call);
  }

  /** Remove a value from room state at a path (see {@link editState}). */
  async removeStatePath(roomId: string, path: string, call: CallOptions = {}): Promise<void> {
    await this.editState(roomId, path, "remove", undefined, call);
  }

  // ------------------------------------------------------------------
  // Capability discovery
  // ------------------------------------------------------------------

  /**
   * Machine-readable catalog of the server's registered room types (with
   * their `filterBy` / `uniqueBy` / `sortBy` knobs), admin RPCs, and core
   * filterable listing fields (GET /admin/api/schema).
   */
  async schema(call: CallOptions = {}): Promise<AdminSchema> {
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
  async call<K extends keyof RpcMap & string>(
    name: K,
    params: RpcMap[K]["params"],
    call: CallOptions = {},
  ): Promise<RpcMap[K]["response"]> {
    return this.callUntyped(name, params, call);
  }

  /**
   * Untyped escape hatch for admin RPCs not declared in the `RpcMap`
   * (or when no map was provided).
   */
  async callUntyped<T = any>(name: string, params?: any, call: CallOptions = {}): Promise<T> {
    return this.request(`/admin/api/rpc/${encodeURIComponent(name)}`, {
      method: "POST",
      body: params ?? null,
      call,
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
  async callRoom<T = any>(roomId: string, name: string, params?: any, call: CallOptions = {}): Promise<T> {
    return this.request(
      `/admin/api/rooms/${encodeURIComponent(roomId)}/rpc/${encodeURIComponent(name)}`,
      { method: "POST", body: params ?? null, call },
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
  roomEvents(
    roomId: string,
    onEvent: (e: RoomEventLog) => void,
    onClose?: () => void,
    opts?: { legacyTokenParam?: boolean },
  ): () => void {
    const wsBase = this.baseUrl.replace(/^http/, "ws");
    let url = `${wsBase}/admin/api/rooms/${encodeURIComponent(roomId)}/events`;
    let protocols: string[] | undefined;
    if (this.token) {
      if (opts?.legacyTokenParam) {
        url += `?token=${encodeURIComponent(this.token)}`;
      } else {
        protocols = [`bearer.${this.token}`];
      }
    }

    const ws = new WebSocket(url, protocols);
    ws.onmessage = (ev) => onEvent(JSON.parse(ev.data as string));
    ws.onclose = onClose ?? null;
    return () => ws.close();
  }

  // ------------------------------------------------------------------
  // Internal
  // ------------------------------------------------------------------

  private async request(
    path: string,
    opts: { method?: string; body?: any; call?: CallOptions } = {},
  ): Promise<any> {
    const headers: Record<string, string> = { "content-type": "application/json" };
    if (this.token) headers.authorization = `Bearer ${this.token}`;
    if (opts.call?.idempotencyKey) headers["idempotency-key"] = opts.call.idempotencyKey;

    const { signal, cancel } = combineAbortSignals(
      opts.call?.timeout ?? this.timeoutMs,
      opts.call?.signal,
    );
    try {
      const res = await this.fetchFn(`${this.baseUrl}${path}`, {
        method: opts.method ?? "GET",
        headers,
        body: opts.body !== undefined ? JSON.stringify(opts.body) : undefined,
        signal,
      });

      if (res.status === 401) throw new AdminError("unauthorized", 401);
      if (!res.ok) {
        const body = (await res.json().catch(() => ({}))) as { error?: string; code?: number };
        throw new AdminError(body.error ?? res.statusText, body.code ?? res.status);
      }
      if (res.status === 204) return null;
      return res.json();
    } finally {
      cancel();
    }
  }
}

/** Serialize a [`RoomQuery`] into URL query params. */
function buildQueryString(query?: RoomQuery): string {
  if (!query) return "";
  const params = new URLSearchParams();
  if (query.name) params.set("name", query.name);
  if (query.filter) serializeFilter(params, query.filter);
  if (query.sort) {
    const parts = Array.isArray(query.sort) ? query.sort : [query.sort];
    params.set(
      "sort",
      parts.map((s) => (s.includes(":") ? s : `${s}:asc`)).join(","),
    );
  }
  if (query.limit !== undefined) params.set("limit", String(query.limit));
  if (query.offset !== undefined) params.set("offset", String(query.offset));
  if (query.count) params.set("count", "true");
  const s = params.toString();
  return s ? `?${s}` : "";
}

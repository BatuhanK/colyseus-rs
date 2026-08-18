import { CallOptions, RoomFilter, RoomQueryResult, RoomListing } from 'colyseus-rs-client';
export { CallOptions, RoomFilter, RoomFilterValue, RoomListing, RoomQueryBuilder, RoomQueryOp, RoomQueryResult } from 'colyseus-rs-client';

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

interface Overview {
    processId: string;
    pid: number;
    uptimeMillis: number;
    rssBytes: number;
    rooms: number;
    connections: number;
}
/** Filtered, paged room query (server-side; GET /admin/api/rooms). */
interface RoomQuery {
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
interface RoomStats {
    total: number;
    open: number;
    waiting: number;
    full: number;
    locked: number;
    private: number;
}
interface ClientInspection {
    sessionId: string;
    state: string;
    auth?: any;
    userData?: any;
}
interface RoomInspection {
    roomId: string;
    state?: any;
    clients: ClientInspection[];
    reservedSeats: number;
    pendingReconnections: number;
    elapsedMillis: number;
    listing: RoomListing;
}
interface RoomEventLog {
    at: number;
    direction: "in" | "out" | "sys";
    kind: string;
    client?: string;
    msgType?: string;
    payload?: any;
    bytes: number;
}
/** A registered room type, as reported by `GET /admin/api/schema`. */
interface RoomTypeSchema {
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
interface AdminRpcSchema {
    name: string;
    /** Rust params type name. */
    params: string;
    /** Rust response type name. */
    response: string;
}
/** Machine-readable capability catalog (GET /admin/api/schema). */
interface AdminSchema {
    roomTypes: RoomTypeSchema[];
    adminRpcs: AdminRpcSchema[];
    coreFilterFields: string[];
}
/**
 * Typed admin RPC catalog: maps RPC name → `{ params, response }`.
 * Pass it as the `AdminClient` generic to make `call()` compile-time checked.
 */
type AdminRpcCatalog = Record<string, {
    params: any;
    response: any;
}>;
declare class AdminError extends Error {
    code?: number;
    constructor(message: string, code?: number);
}
interface AdminClientOptions {
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
declare class AdminClient<RpcMap extends AdminRpcCatalog = {}> {
    private baseUrl;
    private token?;
    private timeoutMs;
    private fetchFn;
    /**
     * Legacy positional form — prefer `new AdminClient({ baseUrl, token, … })`.
     *
     * @param baseUrl   http(s) base url of the game server
     * @param token     the admin bearer token
     * @param timeoutMs default fetch timeout (default 10s)
     */
    constructor(baseUrl: string, token?: string, timeoutMs?: number);
    constructor(options: AdminClientOptions);
    setToken(token: string): void;
    /** Process stats + room listings. */
    overview(call?: CallOptions): Promise<Overview>;
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
    listRooms(query?: RoomQuery, call?: CallOptions): Promise<RoomQueryResult>;
    /** All rooms of a type (auto-paginated) — for dashboards / panels. */
    listRoomsAll(name?: string, call?: CallOptions): Promise<RoomListing[]>;
    /** Per-room-type status counts (open / waiting / full / locked …). */
    roomStats(name?: string, call?: CallOptions): Promise<RoomStats>;
    /**
     * First room of type `name` matching `filter` — e.g. a room waiting for an
     * opponent: `findWaitingRoom("tictactoe", { clients: 1 })`.
     */
    findWaitingRoom(name: string, filter: RoomFilter, call?: CallOptions): Promise<RoomListing | undefined>;
    /** Inspect a room: full state, clients, seats, reconnections. */
    inspectRoom(roomId: string, call?: CallOptions): Promise<RoomInspection>;
    /** Lock a room against new seat reservations. */
    lockRoom(roomId: string, call?: CallOptions): Promise<void>;
    /** Unlock a room. */
    unlockRoom(roomId: string, call?: CallOptions): Promise<void>;
    /** Force-disconnect a client. */
    kick(roomId: string, sessionId: string, call?: CallOptions): Promise<void>;
    /**
     * Send a message to one client (`sessionId` set) or broadcast to all
     * (`sessionId` omitted). `type` is a string or number.
     */
    sendMessage(roomId: string, type: string | number, data: any, sessionId?: string, call?: CallOptions): Promise<void>;
    /** Dispose a room (all clients disconnected). */
    disposeRoom(roomId: string, call?: CallOptions): Promise<void>;
    /**
     * Edit room state at a JSON-pointer-ish path.
     * @param path e.g. "/players/abc123/score" (numeric segments index arrays)
     * @param op   "set" or "remove"
     */
    editState(roomId: string, path: string, op: "set" | "remove", value?: any, call?: CallOptions): Promise<void>;
    /** Set a value in room state at a path (see {@link editState}). */
    setStatePath(roomId: string, path: string, value: any, call?: CallOptions): Promise<void>;
    /** Remove a value from room state at a path (see {@link editState}). */
    removeStatePath(roomId: string, path: string, call?: CallOptions): Promise<void>;
    /**
     * Machine-readable catalog of the server's registered room types (with
     * their `filterBy` / `uniqueBy` / `sortBy` knobs), admin RPCs, and core
     * filterable listing fields (GET /admin/api/schema).
     */
    schema(call?: CallOptions): Promise<AdminSchema>;
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
    call<K extends keyof RpcMap & string>(name: K, params: RpcMap[K]["params"], call?: CallOptions): Promise<RpcMap[K]["response"]>;
    /**
     * Untyped escape hatch for admin RPCs not declared in the `RpcMap`
     * (or when no map was provided).
     */
    callUntyped<T = any>(name: string, params?: any, call?: CallOptions): Promise<T>;
    /**
     * Call a room-based RPC registered server-side via `Server::room_rpc`. The
     * handler runs on the room actor with typed `&mut MyRoom` access and returns
     * its response.
     *
     * ```ts
     * const score = await admin.callRoom<{ player: string; points: number }>(roomId, "getScore", { player: "p1" });
     * ```
     */
    callRoom<T = any>(roomId: string, name: string, params?: any, call?: CallOptions): Promise<T>;
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
    roomEvents(roomId: string, onEvent: (e: RoomEventLog) => void, onClose?: () => void, opts?: {
        legacyTokenParam?: boolean;
    }): () => void;
    private request;
}

export { AdminClient, type AdminClientOptions, AdminError, type AdminRpcCatalog, type AdminRpcSchema, type AdminSchema, type ClientInspection, type Overview, type RoomEventLog, type RoomInspection, type RoomQuery, type RoomStats, type RoomTypeSchema };

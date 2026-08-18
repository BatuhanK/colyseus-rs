/**
 * Room listing types and the server-side room query builder, shared by the
 * player-facing `Client` and the `AdminClient`.
 *
 * The query builder serializes to the `field.op=value` URL param format the
 * server parses with `RoomQuery::from_params`:
 *
 * ```text
 * name=trivia&clients.lt=2&mode.in=a,b&locked.exists=false
 * sort=createdAt:desc,clients:asc&limit=20&offset=0&count=true
 * ```
 */
/** The public, matchmaking-visible state of a room (canonical definition). */
interface RoomListing {
    roomId: string;
    name: string;
    processId: string;
    clients: number;
    maxClients?: number;
    locked: boolean;
    private: boolean;
    metadata?: any;
    createdAt: number;
    [key: string]: any;
}
/** A page of room listings (GET /rooms, GET /admin/api/rooms). */
interface RoomQueryResult {
    items: RoomListing[];
    /** Number of rooms matching the query (before pagination). */
    total: number;
    /** Echo of the requested limit (`null` = unlimited). */
    limit: number | null;
    offset: number;
    /** Offset of the next page, `null` when there is none. */
    nextOffset: number | null;
}
/** A single filter value: a bare primitive (equality) or an operator object. */
type RoomFilterValue = string | number | boolean | {
    eq?: string | number | boolean;
    ne?: string | number | boolean;
    gt?: number;
    gte?: number;
    lt?: number;
    lte?: number;
    in?: (string | number)[];
    exists?: boolean;
    notExists?: boolean;
};
/** A field filter map, e.g. `{ slug: "abc", clients: { lt: 2 } }`. */
type RoomFilter = Record<string, RoomFilterValue>;
/** Operators supported by the server-side room query / matchmaking filter. */
type RoomQueryOp = "eq" | "ne" | "gt" | "gte" | "lt" | "lte" | "in" | "exists" | "notExists";
/**
 * Chainable builder for server-side room queries.
 *
 * ```ts
 * const page = await client.rooms((q) =>
 *   q.name("trivia").where("clients", "lt", 2).sort("createdAt", "desc").limit(20),
 * );
 * ```
 */
declare class RoomQueryBuilder {
    private params;
    private sortKeys;
    /** Restrict to a single room type name. */
    name(name: string): this;
    /** Field exists / does not exist. */
    where(field: string, op: "exists" | "notExists"): this;
    /** Field value is one of `values`. */
    where(field: string, op: "in", value: (string | number)[]): this;
    /** Field comparison (`eq` may be omitted via {@link whereEq}). */
    where(field: string, op: "eq" | "ne" | "gt" | "gte" | "lt" | "lte", value: string | number | boolean): this;
    /** Equality shorthand: `whereEq("slug", "abc")` → `slug=abc`. */
    whereEq(field: string, value: string | number | boolean): this;
    /** Add a sort key (repeatable; earlier keys win). */
    sort(field: string, direction?: "asc" | "desc"): this;
    limit(limit: number): this;
    offset(offset: number): this;
    /** Only compute `total` (items come back empty). */
    countOnly(): this;
    /** The serialized query string, without the leading `?`. */
    toString(): string;
}
/**
 * Serialize an object-style filter map into `field.op=value` params — the
 * same wire format {@link RoomQueryBuilder} produces.
 */
declare function serializeFilter(params: URLSearchParams, filter: RoomFilter): void;

/**
 * Shared internals for the client / admin SDKs. Not part of the public API
 * (not re-exported from `index.ts`).
 */
/** Per-call options accepted by SDK methods that hit the server over HTTP. */
interface CallOptions {
    /** Abort the request after this many milliseconds. */
    timeout?: number;
    /** Caller-provided abort signal, combined with the timeout. */
    signal?: AbortSignal;
    /**
     * `Idempotency-Key` header — the server replays the first response for a
     * duplicate key (matchmake calls and admin RPCs, 30s window).
     */
    idempotencyKey?: string;
}
/**
 * Combine a timeout and a caller signal into one AbortController. Call
 * `cancel()` once the request settles to clear the timer/listener.
 */
declare function combineAbortSignals(timeoutMs: number | undefined, signal: AbortSignal | undefined): {
    signal: AbortSignal | undefined;
    cancel: () => void;
};
/** RFC-4122 UUID via `crypto.randomUUID`, with a getRandomValues fallback. */
declare function randomIdempotencyKey(): string;

/**
 * Minimal TypeScript client for colyseus-rs.
 *
 * - Matchmaking over HTTP (`POST /matchmake/{method}/{roomName}`)
 * - Binary MessagePack frames over WebSocket
 * - Room state applied via JSON-Patch (RFC 6902)
 *
 * ```ts
 * import { Client } from "./client";
 *
 * const client = new Client("http://localhost:2567");
 * const room = await client.joinOrCreate("game", { mode: "ranked" });
 *
 * room.onStateChange((state) => render(state));
 * room.onMessage("chat", (msg) => console.log(msg));
 * room.send("move", { vx: 1, vy: 0 });
 *
 * // reconnection is automatic-ish: keep the token and call client.reconnect(room)
 * ```
 *
 * Only dependency: `@msgpack/msgpack`.
 */

interface SeatReservation {
    room: RoomListing;
    sessionId: string;
    reconnectionToken?: string;
    publicAddress?: string;
    processId: string;
}
declare class MatchmakeError extends Error {
    code: number;
    constructor(code: number, message: string);
}
type MessageHandler = (payload: any) => void;
type StateHandler = (state: any) => void;
declare class Room {
    state: any;
    private ws;
    private messageHandlers;
    private stateHandlers;
    private leaveHandlers;
    private errorHandlers;
    /** Token for reconnection, delivered in the join handshake. */
    reconnectionToken?: string;
    /** "json-patch" when the room has synchronized state, "none" otherwise. */
    serializerId?: string;
    private baseUrl;
    readonly reservation: SeatReservation;
    constructor(
    /** base http(s) url of the server */
    baseUrl: string, reservation: SeatReservation);
    /** @internal */
    connect(reconnectionToken?: string): Promise<void>;
    send(type: string | number, payload?: any): void;
    sendBytes(type: string | number, bytes: Uint8Array): void;
    ping(): void;
    leave(): void;
    /** Close the socket without a consented LEAVE (triggers on_drop server-side). */
    close(): void;
    onMessage(type: string | number, handler: MessageHandler): void;
    onStateChange(handler: StateHandler): void;
    /**
     * Notify state handlers with a deep copy. The internal state object is
     * mutated in place by patches — handing it out directly would alias
     * previous snapshots (breaking change detection in React & friends).
     */
    private emitState;
    onLeave(handler: (code: number) => void): void;
    onError(handler: (code: number, message: string) => void): void;
}
/** Per-call options for the matchmaking methods (`joinOrCreate` & friends). */
interface MatchmakeOptions extends CallOptions {
    /**
     * Server-side room filter, applied by `joinOrCreate` / `join`:
     * `{ clients: { lt: 2 }, slug: "abc", mode: { in: ["a", "b"] } }`.
     * Accepted by `create` for symmetry but ignored (create always makes a
     * new room; `joinById` targets a specific one) — matching server behavior.
     */
    filter?: RoomFilter;
}
declare class Client {
    private baseUrl;
    private getHeaders?;
    /**
     * @param baseUrl http(s) base url of the game server
     * @param getHeaders optional extra headers for matchmaking requests
     *        (e.g. `() => ({ authorization: \`Bearer \${token}\` })`)
     */
    constructor(baseUrl: string, getHeaders?: () => Record<string, string>);
    joinOrCreate(roomName: string, options?: any, call?: MatchmakeOptions): Promise<Room>;
    create(roomName: string, options?: any, call?: MatchmakeOptions): Promise<Room>;
    join(roomName: string, options?: any, call?: MatchmakeOptions): Promise<Room>;
    joinById(roomId: string, options?: any, call?: CallOptions): Promise<Room>;
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
    rooms(query?: string | ((q: RoomQueryBuilder) => void | RoomQueryBuilder)): Promise<RoomQueryResult>;
    private matchmake;
    /** Reconnect into a room after a drop (server must have called allow_reconnection). */
    reconnect(room: Room, call?: CallOptions): Promise<Room>;
    /**
     * Reconnect with a raw roomId + token — e.g. after a page reload, where the
     * previous Room object no longer exists (persist the token in
     * sessionStorage if you want F5-proof sessions).
     */
    reconnectById(roomId: string, reconnectionToken: string, call?: CallOptions): Promise<Room>;
}

export { type CallOptions, Client, MatchmakeError, type MatchmakeOptions, Room, type RoomFilter, type RoomFilterValue, type RoomListing, RoomQueryBuilder, type RoomQueryOp, type RoomQueryResult, type SeatReservation, combineAbortSignals, randomIdempotencyKey, serializeFilter };

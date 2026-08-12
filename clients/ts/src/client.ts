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

import { encode, decode } from "@msgpack/msgpack";

// protocol codes (must match the server)
const JOIN_ROOM = 10;
const ERROR = 11;
const LEAVE_ROOM = 12;
const ROOM_DATA = 13;
const ROOM_STATE = 14;
const ROOM_STATE_PATCH = 15;
const ROOM_DATA_BYTES = 17;
const PING = 18;

export interface RoomListing {
  roomId: string;
  name: string;
  processId: string;
  clients: number;
  maxClients?: number;
  locked: boolean;
  private: boolean;
  metadata?: any;
  createdAt: number;
  [key: string]: any; // filter_by fields
}

export interface SeatReservation {
  room: RoomListing;
  sessionId: string;
  reconnectionToken?: string;
  publicAddress?: string;
  processId: string;
}

export class MatchmakeError extends Error {
  code: number;
  constructor(code: number, message: string) {
    super(message);
    this.code = code;
  }
}

type MessageHandler = (payload: any) => void;
type StateHandler = (state: any) => void;

export class Room {
  state: any = undefined;

  private ws!: WebSocket;
  private messageHandlers = new Map<string | number, MessageHandler[]>();
  private stateHandlers: StateHandler[] = [];
  private leaveHandlers: Array<(code: number) => void> = [];
  private errorHandlers: Array<(code: number, message: string) => void> = [];

  /** Token for reconnection, delivered in the join handshake. */
  reconnectionToken?: string;
  /** "json-patch" when the room has synchronized state, "none" otherwise. */
  serializerId?: string;

  private baseUrl: string;
  readonly reservation: SeatReservation;

  constructor(
    /** base http(s) url of the server */
    baseUrl: string,
    reservation: SeatReservation,
  ) {
    this.baseUrl = baseUrl;
    this.reservation = reservation;
  }

  /** @internal */
  connect(reconnectionToken?: string): Promise<void> {
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
        const bytes = new Uint8Array(ev.data as ArrayBuffer);

        // single-byte ping/pong
        if (bytes.length === 1 && bytes[0] === PING) return;

        const frame = decode(bytes) as any[];
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
          if (this.state === undefined) this.state = {};
          applyPatch(this.state, frame[1]);
          this.emitState();
        } else if (code === ROOM_DATA || code === ROOM_DATA_BYTES) {
          const type = frame[1] as string | number;
          for (const h of this.messageHandlers.get(type) ?? []) h(frame[2]);
          for (const h of this.messageHandlers.get("*") ?? []) h([type, frame[2]]);
        }
      };
    });
  }

  send(type: string | number, payload?: any) {
    this.ws.send(encode([ROOM_DATA, type, payload ?? null]));
  }

  sendBytes(type: string | number, bytes: Uint8Array) {
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

  onMessage(type: string | number, handler: MessageHandler) {
    const list = this.messageHandlers.get(type) ?? [];
    list.push(handler);
    this.messageHandlers.set(type, list);
  }

  onStateChange(handler: StateHandler) {
    this.stateHandlers.push(handler);
  }

  /**
   * Notify state handlers with a deep copy. The internal state object is
   * mutated in place by patches — handing it out directly would alias
   * previous snapshots (breaking change detection in React & friends).
   */
  private emitState() {
    const snapshot = structuredClone(this.state);
    for (const h of this.stateHandlers) h(snapshot);
  }

  onLeave(handler: (code: number) => void) {
    this.leaveHandlers.push(handler);
  }

  onError(handler: (code: number, message: string) => void) {
    this.errorHandlers.push(handler);
  }
}

export class Client {
  private baseUrl: string;
  private getHeaders?: () => Record<string, string>;

  /**
   * @param baseUrl http(s) base url of the game server
   * @param getHeaders optional extra headers for matchmaking requests
   *        (e.g. `() => ({ authorization: \`Bearer \${token}\` })`)
   */
  constructor(baseUrl: string, getHeaders?: () => Record<string, string>) {
    this.baseUrl = baseUrl;
    this.getHeaders = getHeaders;
  }

  joinOrCreate(roomName: string, options: any = {}) {
    return this.matchmake("joinOrCreate", roomName, options);
  }
  create(roomName: string, options: any = {}) {
    return this.matchmake("create", roomName, options);
  }
  join(roomName: string, options: any = {}) {
    return this.matchmake("join", roomName, options);
  }
  joinById(roomId: string, options: any = {}) {
    return this.matchmake("joinById", roomId, options);
  }

  /** List public rooms (optionally filtered by room name). */
  async rooms(roomName?: string): Promise<RoomListing[]> {
    const res = await fetch(`${this.baseUrl}/rooms${roomName ? `/${roomName}` : ""}`);
    return res.json();
  }

  private async matchmake(method: string, roomName: string, options: any): Promise<Room> {
    const res = await fetch(`${this.baseUrl}/matchmake/${method}/${roomName}`, {
      method: "POST",
      headers: { "content-type": "application/json", ...this.getHeaders?.() },
      body: JSON.stringify(options ?? {}),
    });
    if (!res.ok) {
      const body = await res.json().catch(() => ({ code: res.status, error: res.statusText }));
      throw new MatchmakeError(body.code, body.error);
    }
    const reservation = (await res.json()) as SeatReservation;
    const room = new Room(this.baseUrl, reservation);
    await room.connect();
    return room;
  }

  /** Reconnect into a room after a drop (server must have called allow_reconnection). */
  async reconnect(room: Room): Promise<Room> {
    if (!room.reconnectionToken) throw new Error("no reconnection token");
    return this.reconnectById(room.reservation.room.roomId, room.reconnectionToken);
  }

  /**
   * Reconnect with a raw roomId + token — e.g. after a page reload, where the
   * previous Room object no longer exists (persist the token in
   * sessionStorage if you want F5-proof sessions).
   */
  async reconnectById(roomId: string, reconnectionToken: string): Promise<Room> {
    const res = await fetch(`${this.baseUrl}/matchmake/reconnect/${roomId}`, {
      method: "POST",
      headers: { "content-type": "application/json", ...this.getHeaders?.() },
      body: JSON.stringify({ reconnectionToken }),
    });
    if (!res.ok) {
      const body = await res.json().catch(() => ({ code: res.status, error: res.statusText }));
      throw new MatchmakeError(body.code, body.error);
    }
    const reservation = (await res.json()) as SeatReservation;
    const room = new Room(this.baseUrl, reservation);
    await room.connect(reconnectionToken);
    return room;
  }
}

// ---------------------------------------------------------------------
// Minimal RFC 6902 JSON-Patch application (add / replace / remove)
// ---------------------------------------------------------------------

function parsePath(path: string): string[] {
  return path
    .split("/")
    .slice(1)
    .map((seg) => seg.replace(/~1/g, "/").replace(/~0/g, "~"));
}

function applyPatch(doc: any, patch: Array<{ op: string; path: string; value?: any }>) {
  for (const op of patch) {
    const segments = parsePath(op.path);
    let parent = doc;
    for (let i = 0; i < segments.length - 1; i++) {
      const key = Array.isArray(parent) ? Number(segments[i]!) : segments[i]!;
      parent = parent[key];
      if (parent === undefined || parent === null) {
        throw new Error(`json-patch: invalid path ${op.path}`);
      }
    }
    const last = segments[segments.length - 1]!;
    const key: string | number = Array.isArray(parent) ? (last === "-" ? parent.length : Number(last)) : last;

    switch (op.op) {
      case "add":
      case "replace":
        if (Array.isArray(parent)) parent.splice(key as number, op.op === "add" ? 0 : 1, op.value);
        else parent[key as string] = op.value;
        break;
      case "remove":
        if (Array.isArray(parent)) parent.splice(key as number, 1);
        else delete parent[key as string];
        break;
      default:
        throw new Error(`json-patch: unsupported op ${op.op}`);
    }
  }
}

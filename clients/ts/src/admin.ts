/**
 * Admin SDK for colyseus-rs servers.
 *
 * Call built-in admin operations and custom RPCs (registered server-side with
 * `Server::admin_rpc`) over the token-guarded `/admin/api/*` HTTP API.
 *
 * ```ts
 * import { AdminClient } from "./admin";
 *
 * const admin = new AdminClient("http://localhost:2567", "backend-secret");
 *
 * const rooms = await admin.listRooms();
 * await admin.kick("roomId", "sessionId");
 *
 * // custom RPC (registered server-side as `server.admin_rpc::<ResetRoom>("resetRoom")`)
 * const res = await admin.call<{ ok: boolean }>("resetRoom", { roomId: "roomId" });
 * ```
 *
 * No dependencies — uses `fetch` and (for the events stream) `WebSocket`.
 */

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

export interface Overview {
  processId: string;
  pid: number;
  uptimeMillis: number;
  rssBytes: number;
  rooms: number;
  connections: number;
  listings: RoomListing[];
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

export class AdminError extends Error {
  code?: number;
  constructor(message: string, code?: number) {
    super(message);
    this.name = "AdminError";
    this.code = code;
  }
}

export class AdminClient {
  private baseUrl: string;
  private token?: string;

  /**
   * @param baseUrl http(s) base url of the game server
   * @param token   the admin bearer token (set with `Server::admin_token` /
   *                `Server::admin_panel` on the server)
   */
  constructor(baseUrl: string, token?: string) {
    this.baseUrl = baseUrl.replace(/\/+$/, "");
    this.token = token;
  }

  setToken(token: string) {
    this.token = token;
  }

  // ------------------------------------------------------------------
  // Built-in admin operations
  // ------------------------------------------------------------------

  /** Process stats + room listings. */
  async overview(): Promise<Overview> {
    return this.request("/admin/api/overview");
  }

  /** List rooms, optionally filtered by room type name. */
  async listRooms(name?: string): Promise<RoomListing[]> {
    const overview = await this.overview();
    return name ? overview.listings.filter((l) => l.name === name) : overview.listings;
  }

  /** Inspect a room: full state, clients, seats, reconnections. */
  async inspectRoom(roomId: string): Promise<RoomInspection> {
    return this.request(`/admin/api/rooms/${encodeURIComponent(roomId)}`);
  }

  /** Lock a room against new seat reservations. */
  async lockRoom(roomId: string): Promise<void> {
    await this.request(`/admin/api/rooms/${encodeURIComponent(roomId)}/lock`, { method: "POST" });
  }

  /** Unlock a room. */
  async unlockRoom(roomId: string): Promise<void> {
    await this.request(`/admin/api/rooms/${encodeURIComponent(roomId)}/unlock`, { method: "POST" });
  }

  /** Force-disconnect a client. */
  async kick(roomId: string, sessionId: string): Promise<void> {
    await this.request(`/admin/api/rooms/${encodeURIComponent(roomId)}/kick`, {
      method: "POST",
      body: { sessionId },
    });
  }

  /**
   * Send a message to one client (`sessionId` set) or broadcast to all
   * (`sessionId` omitted). `type` is a string or number.
   */
  async sendMessage(roomId: string, type: string | number, data: any, sessionId?: string): Promise<void> {
    await this.request(`/admin/api/rooms/${encodeURIComponent(roomId)}/message`, {
      method: "POST",
      body: { sessionId: sessionId ?? null, type, data },
    });
  }

  /** Dispose a room (all clients disconnected). */
  async disposeRoom(roomId: string): Promise<void> {
    await this.request(`/admin/api/rooms/${encodeURIComponent(roomId)}/dispose`, { method: "POST" });
  }

  /**
   * Edit room state at a JSON-pointer-ish path.
   * @param path e.g. "/players/abc123/score" (numeric segments index arrays)
   * @param op   "set" or "remove"
   */
  async editState(roomId: string, path: string, op: "set" | "remove", value?: any): Promise<void> {
    await this.request(`/admin/api/rooms/${encodeURIComponent(roomId)}/state`, {
      method: "POST",
      body: { path, op, value },
    });
  }

  /** Set a value in room state at a path (see {@link editState}). */
  async setStatePath(roomId: string, path: string, value: any): Promise<void> {
    await this.editState(roomId, path, "set", value);
  }

  /** Remove a value from room state at a path (see {@link editState}). */
  async removeStatePath(roomId: string, path: string): Promise<void> {
    await this.editState(roomId, path, "remove");
  }

  // ------------------------------------------------------------------
  // Custom RPCs
  // ------------------------------------------------------------------

  /**
   * Call a custom admin RPC registered server-side via `Server::admin_rpc`.
   *
   * ```ts
   * const res = await admin.call<{ ok: boolean }>("resetRoom", { roomId });
   * ```
   */
  async call<T = any>(name: string, params?: any): Promise<T> {
    return this.request(`/admin/api/rpc/${encodeURIComponent(name)}`, {
      method: "POST",
      body: params ?? null,
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
  async callRoom<T = any>(roomId: string, name: string, params?: any): Promise<T> {
    return this.request(
      `/admin/api/rooms/${encodeURIComponent(roomId)}/rpc/${encodeURIComponent(name)}`,
      { method: "POST", body: params ?? null },
    );
  }

  // ------------------------------------------------------------------
  // Live events stream
  // ------------------------------------------------------------------

  /**
   * Subscribe to a room's decoded traffic stream (joins, leaves, messages,
   * broadcasts, state patches). Returns a function that closes the socket.
   *
   * The token is passed as a query param (browsers can't set WS headers).
   */
  roomEvents(roomId: string, onEvent: (e: RoomEventLog) => void, onClose?: () => void): () => void {
    const wsBase = this.baseUrl.replace(/^http/, "ws");
    let url = `${wsBase}/admin/api/rooms/${encodeURIComponent(roomId)}/events`;
    if (this.token) url += `?token=${encodeURIComponent(this.token)}`;

    const ws = new WebSocket(url);
    ws.onmessage = (ev) => onEvent(JSON.parse(ev.data as string));
    ws.onclose = onClose ?? null;
    return () => ws.close();
  }

  // ------------------------------------------------------------------
  // Internal
  // ------------------------------------------------------------------

  private async request(path: string, opts: { method?: string; body?: any } = {}): Promise<any> {
    const headers: Record<string, string> = { "content-type": "application/json" };
    if (this.token) headers.authorization = `Bearer ${this.token}`;

    const res = await fetch(`${this.baseUrl}${path}`, {
      method: opts.method ?? "GET",
      headers,
      body: opts.body !== undefined ? JSON.stringify(opts.body) : undefined,
    });

    if (res.status === 401) throw new AdminError("unauthorized", 401);
    if (!res.ok) {
      const body = await res.json().catch(() => ({ error: res.statusText }));
      throw new AdminError(body.error ?? res.statusText, body.code ?? res.status);
    }
    if (res.status === 204) return null;
    return res.json();
  }
}

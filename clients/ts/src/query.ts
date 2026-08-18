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

/** A page of room listings (GET /rooms, GET /admin/api/rooms). */
export interface RoomQueryResult {
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
export type RoomFilterValue =
  | string
  | number
  | boolean
  | {
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
export type RoomFilter = Record<string, RoomFilterValue>;

/** Operators supported by the server-side room query / matchmaking filter. */
export type RoomQueryOp =
  | "eq"
  | "ne"
  | "gt"
  | "gte"
  | "lt"
  | "lte"
  | "in"
  | "exists"
  | "notExists";

/**
 * Chainable builder for server-side room queries.
 *
 * ```ts
 * const page = await client.rooms((q) =>
 *   q.name("trivia").where("clients", "lt", 2).sort("createdAt", "desc").limit(20),
 * );
 * ```
 */
export class RoomQueryBuilder {
  private params = new URLSearchParams();
  private sortKeys: string[] = [];

  /** Restrict to a single room type name. */
  name(name: string): this {
    this.params.set("name", name);
    return this;
  }

  /** Field exists / does not exist. */
  where(field: string, op: "exists" | "notExists"): this;
  /** Field value is one of `values`. */
  where(field: string, op: "in", value: (string | number)[]): this;
  /** Field comparison (`eq` may be omitted via {@link whereEq}). */
  where(
    field: string,
    op: "eq" | "ne" | "gt" | "gte" | "lt" | "lte",
    value: string | number | boolean,
  ): this;
  where(field: string, op: RoomQueryOp, value?: string | number | boolean | (string | number)[]): this {
    switch (op) {
      case "eq":
        this.params.set(field, String(value));
        break;
      case "in":
        this.params.set(`${field}.in`, (value as (string | number)[]).join(","));
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
  whereEq(field: string, value: string | number | boolean): this {
    return this.where(field, "eq", value);
  }

  /** Add a sort key (repeatable; earlier keys win). */
  sort(field: string, direction: "asc" | "desc" = "asc"): this {
    this.sortKeys.push(`${field}:${direction}`);
    return this;
  }

  limit(limit: number): this {
    this.params.set("limit", String(limit));
    return this;
  }

  offset(offset: number): this {
    this.params.set("offset", String(offset));
    return this;
  }

  /** Only compute `total` (items come back empty). */
  countOnly(): this {
    this.params.set("count", "true");
    return this;
  }

  /** The serialized query string, without the leading `?`. */
  toString(): string {
    if (this.sortKeys.length > 0) this.params.set("sort", this.sortKeys.join(","));
    return this.params.toString();
  }
}

/**
 * Serialize an object-style filter map into `field.op=value` params — the
 * same wire format {@link RoomQueryBuilder} produces.
 */
export function serializeFilter(params: URLSearchParams, filter: RoomFilter): void {
  for (const [field, value] of Object.entries(filter)) {
    if (value === undefined || value === null) continue;
    if (typeof value === "object") {
      for (const [op, v] of Object.entries(value)) {
        if (v === undefined) continue;
        if (op === "in") params.set(`${field}.in`, Array.isArray(v) ? v.join(",") : String(v));
        else if (op === "notExists") params.set(`${field}.exists`, String(!v));
        else params.set(`${field}.${op}`, String(v));
      }
    } else {
      params.set(field, String(value));
    }
  }
}

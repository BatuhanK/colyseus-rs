export * from "./client";
export * from "./admin";
// both client.ts and admin.ts declare RoomListing — pin the canonical one.
export { type RoomListing } from "./admin";

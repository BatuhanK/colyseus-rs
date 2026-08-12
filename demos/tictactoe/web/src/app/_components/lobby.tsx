"use client";

import Link from "next/link";
import { useRouter } from "next/navigation";
import { useEffect, useState } from "react";

import { env } from "~/env";

interface RoomListing {
  roomId: string;
  name: string;
  clients: number;
  maxClients?: number;
  locked: boolean;
  createdAt: number;
}

export function Lobby() {
  const router = useRouter();
  const [rooms, setRooms] = useState<RoomListing[] | null>(null);
  const [error, setError] = useState<string | null>(null);

  const refresh = async () => {
    try {
      const res = await fetch(
        `${env.NEXT_PUBLIC_GAME_URL}/rooms/tictactoe`,
        { cache: "no-store" },
      );
      setRooms((await res.json()) as RoomListing[]);
      setError(null);
    } catch {
      setError("game server unreachable");
    }
  };

  useEffect(() => {
    void refresh();
    const t = setInterval(refresh, 3000);
    return () => clearInterval(t);
  }, []);

  return (
    <div className="mx-auto flex max-w-xl flex-col gap-6 p-6">
      <button
        onClick={() => router.push("/room/new")}
        className="rounded-md bg-white py-3 text-lg font-semibold text-black hover:bg-neutral-200"
      >
        + new game
      </button>

      {error && <p className="text-red-400">{error}</p>}

      <div className="flex flex-col gap-2">
        <h2 className="text-sm font-semibold uppercase tracking-wide text-neutral-500">
          open games
        </h2>
        {rooms?.length === 0 && (
          <p className="text-neutral-500">no open games — create one!</p>
        )}
        {rooms?.map((room) => (
          <Link
            key={room.roomId}
            href={`/room/${room.roomId}`}
            className="flex items-center justify-between rounded-md border border-neutral-800 bg-neutral-900 px-4 py-3 hover:border-neutral-600"
          >
            <span className="font-mono text-sm">{room.roomId}</span>
            <span className="text-neutral-400">
              {room.clients}/{room.maxClients ?? "∞"} players
            </span>
          </Link>
        ))}
        {rooms === null && !error && (
          <p className="text-neutral-500">loading…</p>
        )}
      </div>
    </div>
  );
}

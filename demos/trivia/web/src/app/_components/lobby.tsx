"use client";

import { useRouter } from "next/navigation";
import { useEffect, useMemo, useState } from "react";

import { Client, type RoomListing } from "colyseus-rs-client";

import { env } from "~/env";
import { api } from "~/trpc/react";

interface TriviaRoomListing extends RoomListing {
  difficulty?: string;
  category?: string;
  metadata?: { phase?: string; round?: number; players?: number; spectators?: number; hasPassword?: boolean };
}

const DIFFICULTIES = ["easy", "medium", "hard"] as const;

const DIFFICULTY_STYLE: Record<string, string> = {
  easy: "bg-emerald-500/15 text-emerald-400 border-emerald-500/30",
  medium: "bg-amber-500/15 text-amber-400 border-amber-500/30",
  hard: "bg-rose-500/15 text-rose-400 border-rose-500/30",
};

const PHASE_LABEL: Record<string, string> = {
  lobby: "in lobby",
  generating: "starting…",
  question: "in game",
  reveal: "in game",
  finished: "finished",
};

export function Lobby() {
  const router = useRouter();
  const [rooms, setRooms] = useState<TriviaRoomListing[] | null>(null);
  const [serverDown, setServerDown] = useState(false);

  const [search, setSearch] = useState("");
  const [difficultyFilter, setDifficultyFilter] = useState<string>("all");

  const [newDifficulty, setNewDifficulty] = useState<string>("easy");
  const [newCategory, setNewCategory] = useState("");
  const [newPassword, setNewPassword] = useState("");

  const leaderboard = api.game.leaderboard.useQuery(undefined, {
    refetchInterval: 5000,
  });

  const client = useMemo(() => new Client(env.NEXT_PUBLIC_GAME_URL), []);

  // Server-side query: difficulty filter + newest-first sort run on the game
  // server; only the substring category search stays client-side (the query
  // language has no contains-op).
  useEffect(() => {
    let dead = false;
    const refresh = async () => {
      try {
        const page = await client.rooms((q) => {
          q.name("trivia").sort("createdAt", "desc").limit(50);
          if (difficultyFilter !== "all") q.whereEq("difficulty", difficultyFilter);
        });
        if (dead) return;
        setRooms(page.items);
        setServerDown(false);
      } catch {
        if (!dead) setServerDown(true);
      }
    };
    void refresh();
    const t = setInterval(refresh, 2500);
    return () => {
      dead = true;
      clearInterval(t);
    };
  }, [client, difficultyFilter]);

  const filtered = useMemo(() => {
    const q = search.trim().toLowerCase();
    return (rooms ?? []).filter(
      (r) => q === "" || (r.category ?? "").toLowerCase().includes(q),
    );
  }, [rooms, search]);

  const createRoom = (e: React.FormEvent) => {
    e.preventDefault();
    const params = new URLSearchParams({
      difficulty: newDifficulty,
      category: newCategory.trim() || "genel",
    });
    if (newPassword.trim()) params.set("password", newPassword.trim());
    router.push(`/room/new?${params.toString()}`);
  };

  return (
    <div className="mx-auto grid max-w-5xl gap-6 p-6 lg:grid-cols-[1fr_280px]">
      <div className="flex flex-col gap-6">
        {/* create room */}
        <form
          onSubmit={createRoom}
          className="flex flex-col gap-3 rounded-xl border border-neutral-800 bg-neutral-900 p-4"
        >
          <h2 className="text-sm font-semibold uppercase tracking-wide text-neutral-500">
            new game
          </h2>
          <div className="flex flex-wrap gap-2">
            {DIFFICULTIES.map((d) => (
              <button
                type="button"
                key={d}
                onClick={() => setNewDifficulty(d)}
                className={`rounded-full border px-4 py-1.5 text-sm font-medium transition ${
                  newDifficulty === d
                    ? DIFFICULTY_STYLE[d]
                    : "border-neutral-700 text-neutral-400 hover:border-neutral-500"
                }`}
              >
                {d}
              </button>
            ))}
          </div>
          <div className="flex gap-2">
            <input
              value={newCategory}
              onChange={(e) => setNewCategory(e.target.value)}
              placeholder="category (e.g. tarih, bilim, futbol…)"
              maxLength={40}
              className="flex-1 rounded-md border border-neutral-700 bg-neutral-950 px-3 py-2 text-sm outline-none focus:border-neutral-500"
            />
            <input
              value={newPassword}
              onChange={(e) => setNewPassword(e.target.value)}
              placeholder="password (optional)"
              maxLength={64}
              className="w-44 rounded-md border border-neutral-700 bg-neutral-950 px-3 py-2 text-sm outline-none focus:border-neutral-500"
            />
            <button
              type="submit"
              className="rounded-md bg-white px-5 py-2 text-sm font-semibold text-black hover:bg-neutral-200"
            >
              create →
            </button>
          </div>
        </form>

        {/* search + filter */}
        <div className="flex flex-wrap items-center gap-2">
          <input
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder="search by category…"
            className="min-w-48 flex-1 rounded-md border border-neutral-700 bg-neutral-900 px-3 py-2 text-sm outline-none focus:border-neutral-500"
          />
          {["all", ...DIFFICULTIES].map((d) => (
            <button
              key={d}
              onClick={() => setDifficultyFilter(d)}
              className={`rounded-full border px-3 py-1.5 text-xs font-medium transition ${
                difficultyFilter === d
                  ? "border-neutral-400 bg-neutral-800 text-neutral-100"
                  : "border-neutral-800 text-neutral-500 hover:border-neutral-600"
              }`}
            >
              {d}
            </button>
          ))}
        </div>

        {/* room list */}
        {serverDown && (
          <p className="rounded-md border border-rose-500/30 bg-rose-500/10 p-3 text-sm text-rose-400">
            game server unreachable — is trivia-server running on :2568?
          </p>
        )}
        <div className="grid gap-3 sm:grid-cols-2">
          {filtered.map((room) => {
            const players = room.metadata?.players ?? 0;
            const phase = room.metadata?.phase ?? "lobby";
            // Always enter wanting to play — the server demotes to spectator
            // when the room is full or mid-game. (Don't pre-decide here:
            // listing data is polled and can be seconds stale.)
            return (
              <button
                key={room.roomId}
                onClick={() => router.push(`/room/${room.roomId}`)}
                className="group flex flex-col gap-2 rounded-xl border border-neutral-800 bg-neutral-900 p-4 text-left transition hover:border-neutral-600"
              >
                <div className="flex items-center justify-between">
                  <span className="truncate text-lg font-semibold">
                    {room.metadata?.hasPassword && <span title="password protected">🔒 </span>}
                    {room.category ?? "genel"}
                  </span>
                  <span
                    className={`rounded-full border px-2.5 py-0.5 text-xs font-medium ${DIFFICULTY_STYLE[room.difficulty ?? "easy"]}`}
                  >
                    {room.difficulty ?? "easy"}
                  </span>
                </div>
                <div className="flex items-center justify-between text-sm text-neutral-400">
                  <span>
                    {players}/4 players
                    {(room.metadata?.spectators ?? 0) > 0 &&
                      ` · ${room.metadata?.spectators} watching`}
                  </span>
                  <span
                    className={`rounded-full px-2 py-0.5 text-xs ${
                      phase === "lobby"
                        ? "bg-sky-500/15 text-sky-400"
                        : phase === "finished"
                          ? "bg-neutral-700/50 text-neutral-400"
                          : "bg-violet-500/15 text-violet-400"
                    }`}
                  >
                    {phase === "question" || phase === "reveal"
                      ? `round ${room.metadata?.round}/10`
                      : PHASE_LABEL[phase]}
                  </span>
                </div>
              </button>
            );
          })}
        </div>
        {rooms && filtered.length === 0 && (
          <p className="text-sm text-neutral-500">no rooms match — create one above!</p>
        )}
      </div>

      {/* leaderboard */}
      <aside className="flex flex-col gap-3 rounded-xl border border-neutral-800 bg-neutral-900 p-4 lg:sticky lg:top-6 lg:self-start">
        <h2 className="text-sm font-semibold uppercase tracking-wide text-neutral-500">
          🏆 leaderboard
        </h2>
        {(leaderboard.data ?? []).length === 0 && (
          <p className="text-sm text-neutral-500">no games played yet</p>
        )}
        {(leaderboard.data ?? []).map((entry, i) => (
          <div
            key={entry.userId}
            className="flex items-center justify-between rounded-md bg-neutral-950 px-3 py-2 text-sm"
          >
            <span className="flex items-center gap-2">
              <span className="w-5 text-neutral-500">
                {i === 0 ? "🥇" : i === 1 ? "🥈" : i === 2 ? "🥉" : `${i + 1}.`}
              </span>
              <span className="font-medium">{entry.name}</span>
            </span>
            <span className="text-neutral-400">
              {entry.wins}W · {entry.totalScore}p
            </span>
          </div>
        ))}
      </aside>
    </div>
  );
}

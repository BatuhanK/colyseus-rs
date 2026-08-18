"use client";

import { useParams, useRouter } from "next/navigation";
import { useCallback, useEffect, useRef, useState } from "react";

import { env } from "~/env";
import { Client, type Room } from "colyseus-rs-client";
import { api } from "~/trpc/react";

interface PlayerInfo {
  symbol: string;
  name: string;
}

interface TttState {
  board: string[];
  players: Record<string, PlayerInfo>;
  turn: string;
  status: "waiting" | "playing" | "finished";
  winner: string | null;
}

interface ChatLine {
  from: string;
  text: string;
}

export default function RoomPage() {
  const params = useParams<{ id: string }>();
  const router = useRouter();
  const roomId = params.id;

  const tokenQuery = api.game.gameToken.useQuery(undefined, {
    refetchOnWindowFocus: false,
    staleTime: 1000 * 60 * 30,
  });

  const [state, setState] = useState<TttState | null>(null);
  const [chat, setChat] = useState<ChatLine[]>([]);
  const [status, setStatus] = useState("connecting…");
  const [draft, setDraft] = useState("");

  const roomRef = useRef<Room | null>(null);
  const sessionIdRef = useRef<string | null>(null);

  const attach = useCallback((room: Room) => {
    roomRef.current = room;
    sessionIdRef.current = room.reservation.sessionId;

    room.onStateChange((s: TttState) => setState({ ...s }));
    room.onMessage("chat", (msg: ChatLine) =>
      setChat((prev) => [...prev.slice(-99), msg]),
    );
    room.onMessage("system", (msg: { text: string }) =>
      setChat((prev) => [...prev.slice(-99), { from: "·", text: msg.text }]),
    );
    room.onError((_code, message) => setStatus(`error: ${message}`));
  }, []);

  useEffect(() => {
    const token = tokenQuery.data?.token;
    if (!token) return;

    let cancelled = false;
    let room: Room | null = null;

    const client = new Client(env.NEXT_PUBLIC_GAME_URL, () => ({
      authorization: `Bearer ${token}`,
    }));

    const connect = async () => {
      setStatus("connecting…");
      const skey = `ttt-session:${roomId}`;
      try {
        if (roomId === "new") {
          room = await client.create("tictactoe", {});
        } else {
          // page reload? resume the previous session if we have a token
          const savedToken = JSON.parse(sessionStorage.getItem(skey) ?? "null")?.token;
          if (savedToken) {
            try {
              room = await client.reconnectById(roomId, savedToken);
            } catch {
              sessionStorage.removeItem(skey);
            }
          }
          room ??= await client.joinById(roomId, {});
        }
        if (cancelled) {
          room.leave();
          return;
        }
        const realRoomId = room.reservation.room.roomId;
        if (roomId === "new") {
          window.history.replaceState(null, "", `/room/${realRoomId}`);
        }
        attach(room);
        if (room.reconnectionToken) {
          sessionStorage.setItem(`ttt-session:${realRoomId}`, JSON.stringify({ token: room.reconnectionToken }));
        }
        setStatus("connected");

        room.onLeave(async (code) => {
          if (cancelled || code === 4000) return;
          setStatus("connection lost, reconnecting…");
          for (let attempt = 0; attempt < 10 && !cancelled; attempt++) {
            try {
              await new Promise((r) => setTimeout(r, 1000));
              const old = room!;
              room = await client.reconnect(old);
              if (cancelled) return;
              attach(room);
              setStatus("connected");
              return;
            } catch {
              // keep trying while the server's reconnection window is open
            }
          }
          if (!cancelled) setStatus("could not reconnect — back to lobby");
        });
      } catch (err) {
        if (!cancelled) {
          setStatus(err instanceof Error ? err.message : "failed to join");
        }
      }
    };

    void connect();
    return () => {
      cancelled = true;
      room?.leave();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [roomId, tokenQuery.data?.token]);

  const me = sessionIdRef.current ? state?.players[sessionIdRef.current] : null;
  const myTurn = state?.status === "playing" && me?.symbol === state.turn;

  const sendChat = (e: React.FormEvent) => {
    e.preventDefault();
    const text = draft.trim();
    if (!text) return;
    roomRef.current?.send("chat", { text });
    setDraft("");
  };

  return (
    <main className="flex min-h-screen flex-col items-center gap-6 bg-neutral-950 p-6 text-neutral-100">
      <div className="flex w-full max-w-3xl items-center justify-between">
        <button
          onClick={() => router.push("/")}
          className="text-sm text-neutral-400 hover:text-neutral-200"
        >
          ← lobby
        </button>
        <span className="text-sm text-neutral-500">{status}</span>
      </div>

      <div className="flex w-full max-w-3xl flex-col gap-6 md:flex-row">
        {/* board */}
        <div className="flex flex-1 flex-col items-center gap-4">
          <div className="text-lg">
            {!state || state.status === "waiting" ? (
              <span className="text-neutral-400">waiting for opponent…</span>
            ) : state.status === "finished" ? (
              <span className="font-bold">
                {state.winner === "draw"
                  ? "draw!"
                  : `${winnerName(state)} wins!`}
              </span>
            ) : (
              <span>
                turn: <b>{state.turn}</b>{" "}
                {me && (
                  <span className="text-neutral-400">
                    (you are {me.symbol}
                    {myTurn ? " — your move" : ""})
                  </span>
                )}
              </span>
            )}
          </div>

          <div className="grid grid-cols-3 gap-2">
            {(state?.board ?? Array(9).fill("")).map((cell, i) => (
              <button
                key={i}
                disabled={!myTurn || cell !== ""}
                onClick={() => roomRef.current?.send("move", { cell: i })}
                className="flex h-24 w-24 items-center justify-center rounded-lg border border-neutral-700 bg-neutral-900 text-5xl font-bold disabled:cursor-not-allowed enabled:hover:bg-neutral-800"
              >
                <span className={cell === "X" ? "text-sky-400" : "text-rose-400"}>
                  {cell}
                </span>
              </button>
            ))}
          </div>

          {state?.status === "finished" && (
            <button
              onClick={() => roomRef.current?.send("rematch", {})}
              className="rounded-md bg-white px-6 py-2 font-semibold text-black hover:bg-neutral-200"
            >
              rematch
            </button>
          )}
        </div>

        {/* chat */}
        <div className="flex w-full flex-col rounded-lg border border-neutral-800 bg-neutral-900 md:w-72">
          <div className="border-b border-neutral-800 px-3 py-2 text-sm font-semibold text-neutral-400">
            chat
          </div>
          <div className="flex h-72 flex-1 flex-col gap-1 overflow-y-auto p-3 text-sm">
            {chat.map((line, i) => (
              <div key={i}>
                <b className="text-neutral-300">{line.from}</b>{" "}
                <span className="text-neutral-400">{line.text}</span>
              </div>
            ))}
          </div>
          <form onSubmit={sendChat} className="border-t border-neutral-800 p-2">
            <input
              value={draft}
              onChange={(e) => setDraft(e.target.value)}
              placeholder="say something…"
              className="w-full rounded-md bg-neutral-950 px-3 py-2 text-sm outline-none focus:ring-1 focus:ring-neutral-600"
            />
          </form>
        </div>
      </div>
    </main>
  );
}

function winnerName(state: TttState): string {
  const winner = Object.values(state.players).find(
    (p) => p.symbol === state.winner,
  );
  return winner?.name ?? state.winner ?? "";
}

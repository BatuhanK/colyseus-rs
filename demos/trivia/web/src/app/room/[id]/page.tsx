"use client";

import { useParams, useRouter, useSearchParams } from "next/navigation";
import { Suspense, useCallback, useEffect, useRef, useState } from "react";

import { env } from "~/env";
import { Client, type Room } from "~/lib/colyseus";
import { api } from "~/trpc/react";

interface PlayerState {
  userId: string;
  name: string;
  ready: boolean;
  score: number;
  correctCount: number;
  answered: boolean;
  choice: number | null;
}

interface TriviaState {
  difficulty: string;
  category: string;
  phase: "lobby" | "generating" | "question" | "reveal" | "finished";
  round: number;
  totalRounds: number;
  owner: string | null;
  players: Record<string, PlayerState>;
  spectators: Record<string, { name: string }>;
  question: { text: string; choices: string[] } | null;
  correctIndex: number | null;
  phaseEndsAt: number | null;
  answersIn: number;
  winners: string[];
}

interface ChatLine {
  from: string;
  text: string;
}

const ROUND_SECONDS = 20;

// F5-proof sessions: keep the reconnection token per room in sessionStorage.
const sessionKey = (roomId: string) => `trivia-session:${roomId}`;
const loadSavedToken = (roomId: string): string | null => {
  try {
    return JSON.parse(sessionStorage.getItem(sessionKey(roomId)) ?? "null")?.token ?? null;
  } catch {
    return null;
  }
};
const saveToken = (roomId: string, token: string | undefined) => {
  if (token) sessionStorage.setItem(sessionKey(roomId), JSON.stringify({ token }));
};
const clearSavedToken = (roomId: string) => sessionStorage.removeItem(sessionKey(roomId));

export default function RoomPage() {
  return (
    <Suspense>
      <RoomInner />
    </Suspense>
  );
}

function RoomInner() {
  const params = useParams<{ id: string }>();
  const searchParams = useSearchParams();
  const router = useRouter();
  const roomId = params.id;

  const tokenQuery = api.game.gameToken.useQuery(undefined, {
    refetchOnWindowFocus: false,
    staleTime: 1000 * 60 * 30,
  });

  const [state, setState] = useState<TriviaState | null>(null);
  const [chat, setChat] = useState<ChatLine[]>([]);
  const [status, setStatus] = useState("connecting…");
  const [draft, setDraft] = useState("");
  const [myPick, setMyPick] = useState<number | null>(null);
  const [now, setNow] = useState(Date.now());

  const roomRef = useRef<Room | null>(null);
  const sessionIdRef = useRef<string | null>(null);

  // countdown ticker
  useEffect(() => {
    const t = setInterval(() => setNow(Date.now()), 200);
    return () => clearInterval(t);
  }, []);

  // new round → clear the locked-in pick
  const phase = state?.phase;
  const round = state?.round;
  useEffect(() => {
    if (phase === "question") setMyPick(null);
  }, [phase, round]);

  const attach = useCallback((room: Room) => {
    roomRef.current = room;
    sessionIdRef.current = room.reservation.sessionId;

    room.onStateChange((s: TriviaState) => setState(s));
    room.onMessage("chat", (msg: ChatLine) =>
      setChat((prev) => [...prev.slice(-99), msg]),
    );
    room.onMessage("system", (msg: { text: string }) =>
      setChat((prev) => [...prev.slice(-99), { from: "·", text: msg.text }]),
    );
    room.onError((_code, message) => setStatus(`error: ${message}`));
  }, []);

  // Shared across StrictMode's double-effect-invoke and re-renders:
  // the connection promise + room live in refs, so the second effect run
  // reuses them instead of creating/joining a second time.
  const connRef = useRef<Promise<Room> | null>(null);
  const effectIdRef = useRef(0);
  const pwRef = useRef("");
  const [pwPrompt, setPwPrompt] = useState<{ error?: string } | null>(null);
  const [pwAttempt, setPwAttempt] = useState(0);

  useEffect(() => {
    const token = tokenQuery.data?.token;
    if (!token) return;

    const myId = ++effectIdRef.current;
    const isCurrent = () => effectIdRef.current === myId;

    const client = new Client(env.NEXT_PUBLIC_GAME_URL, () => ({
      authorization: `Bearer ${token}`,
    }));

    const actuallyConnect = async (): Promise<Room> => {
      if (roomId === "new") {
        return client.create("trivia", {
          difficulty: searchParams.get("difficulty") ?? "easy",
          category: searchParams.get("category") ?? "genel",
          ...(searchParams.get("password")
            ? { password: searchParams.get("password") }
            : {}),
        });
      }
      const savedToken = loadSavedToken(roomId);
      if (savedToken) {
        try {
          return await client.reconnectById(roomId, savedToken);
        } catch {
          clearSavedToken(roomId); // expired/disposed — fall through to fresh join
        }
      }
      return client.joinById(roomId, {
        role: searchParams.get("role") === "spectator" ? "spectator" : "player",
        ...(pwRef.current ? { password: pwRef.current } : {}),
      });
    };

    const setup = (room: Room) => {
      if (!isCurrent()) return;
      roomRef.current = room;
      const realRoomId = room.reservation.room.roomId;
      if (roomId === "new") {
        const q = searchParams.toString();
        window.history.replaceState(null, "", `/room/${realRoomId}${q ? `?${q}` : ""}`);
      }
      attach(room);
      saveToken(realRoomId, room.reconnectionToken);
      setStatus("");

      room.onLeave(async (code) => {
        if (!isCurrent() || code === 4000) return;
        setStatus("connection lost, reconnecting…");
        for (let attempt = 0; attempt < 5 && isCurrent(); attempt++) {
          try {
            await new Promise((r) => setTimeout(r, 1000));
            const fresh = await client.reconnect(room);
            if (!isCurrent()) return;
            room = fresh;
            attach(room);
            saveToken(realRoomId, room.reconnectionToken);
            setStatus("");
            return;
          } catch {
            // retry while the server's reconnection window is open
          }
        }
        if (!isCurrent()) return;
        // reconnect failed (e.g. dropped in the lobby → seat was released).
        // fall back to a fresh join so the user lands back in the room.
        clearSavedToken(realRoomId);
        try {
          setStatus("rejoining…");
          const fresh = await client.joinById(realRoomId, {
            role: searchParams.get("role") === "spectator" ? "spectator" : "player",
            ...(pwRef.current ? { password: pwRef.current } : {}),
          });
          if (!isCurrent()) {
            fresh.leave();
            return;
          }
          room = fresh;
          attach(room);
          saveToken(realRoomId, room.reconnectionToken);
          setStatus("");
        } catch {
          if (isCurrent()) setStatus("could not reconnect");
        }
      });
    };

    // reuse an existing live connection (StrictMode second run / re-render)
    if (roomRef.current) {
      setup(roomRef.current);
    } else {
      setStatus("connecting…");
      connRef.current ??= actuallyConnect();
      connRef.current
        .then((room) => {
          connRef.current = null;
          setup(room);
        })
        .catch((err) => {
          connRef.current = null;
          if (!isCurrent()) return;
          const message = err instanceof Error ? err.message : "failed to join";
          if (message.includes("password")) {
            setPwPrompt({ error: message });
          } else {
            setStatus(message);
          }
        });
    }

    return () => {
      // StrictMode dev unmount is followed immediately by a re-run (myId
      // bumps), which cancels this timer — so the room is only left on a
      // REAL unmount.
      const idAtCleanup = myId;
      setTimeout(() => {
        if (effectIdRef.current === idAtCleanup) {
          roomRef.current?.leave();
          roomRef.current = null;
        }
      }, 300);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [roomId, tokenQuery.data?.token, pwAttempt]);

  const me = sessionIdRef.current ? state?.players[sessionIdRef.current] : undefined;
  const isSpectator = !me;
  const isOwner = state?.owner === sessionIdRef.current;
  const players = Object.entries(state?.players ?? {});
  const allReady = players.length > 0 && players.every(([, p]) => p.ready);

  const secondsLeft =
    state?.phaseEndsAt != null
      ? Math.max(0, (state.phaseEndsAt - now) / 1000)
      : null;
  const timeFraction =
    state?.phase === "question" && secondsLeft != null
      ? secondsLeft / ROUND_SECONDS
      : null;

  const sendChat = (e: React.FormEvent) => {
    e.preventDefault();
    const text = draft.trim();
    if (!text) return;
    roomRef.current?.send("chat", { text });
    setDraft("");
  };

  const answer = (choice: number) => {
    if (myPick !== null) return;
    setMyPick(choice);
    roomRef.current?.send("answer", { choice });
  };

  return (
    <main className="flex min-h-screen flex-col bg-neutral-950 text-neutral-100">
      {pwPrompt && (
        <div
          className="bg-black/60"
          style={{ position: "fixed", top: 0, left: 0, right: 0, bottom: 0, zIndex: 30 }}
        >
          <form
            onSubmit={(e) => {
              e.preventDefault();
              const value = new FormData(e.currentTarget).get("pw") as string;
              pwRef.current = value;
              setPwPrompt(null);
              setPwAttempt((n) => n + 1);
            }}
            className="flex w-72 flex-col gap-3 rounded-xl border border-neutral-800 bg-neutral-900 p-6"
            style={{
              position: "fixed",
              top: "50%",
              left: "50%",
              transform: "translate(-50%, -50%)",
            }}
          >
            <h2 className="font-bold">🔒 room password</h2>
            {pwPrompt.error && (
              <p className="text-sm text-rose-400">{pwPrompt.error}</p>
            )}
            <input
              name="pw"
              autoFocus
              placeholder="password"
              className="rounded-md border border-neutral-700 bg-neutral-950 px-3 py-2 text-sm outline-none focus:border-neutral-500"
            />
            <button className="rounded-md bg-white py-2 text-sm font-semibold text-black hover:bg-neutral-200">
              join
            </button>
          </form>
        </div>
      )}
      <header className="flex items-center justify-between border-b border-neutral-800 px-6 py-3">
        <div className="flex items-center gap-3">
          <button
            onClick={() => router.push("/")}
            className="text-sm text-neutral-400 hover:text-neutral-200"
          >
            ← lobby
          </button>
          {state && (
            <span className="text-sm text-neutral-500">
              {state.category} · {state.difficulty}
            </span>
          )}
        </div>
        {status && <span className="text-sm text-amber-400">{status}</span>}
        {state && state.phase !== "lobby" && state.phase !== "finished" && (
          <span className="rounded-full bg-neutral-800 px-3 py-1 text-sm font-medium">
            round {Math.max(state.round, 1)}/{state.totalRounds}
          </span>
        )}
      </header>

      <div className="mx-auto grid w-full max-w-5xl flex-1 gap-6 p-6 lg:grid-cols-[1fr_300px]">
        <div className="flex flex-col gap-4">
          {/* ------------ lobby ------------ */}
          {state?.phase === "lobby" && (
            <div className="flex flex-col items-center gap-6 rounded-xl border border-neutral-800 bg-neutral-900 p-8">
              <h2 className="text-xl font-bold">waiting for players</h2>
              <div className="flex flex-wrap justify-center gap-3">
                {players.map(([sid, p]) => (
                  <div
                    key={sid}
                    className={`flex items-center gap-2 rounded-lg border px-4 py-2 ${
                      p.ready
                        ? "border-emerald-500/40 bg-emerald-500/10"
                        : "border-neutral-700 bg-neutral-950"
                    }`}
                  >
                    <span className="font-medium">{p.name}</span>
                    {sid === state.owner && <span title="owner">👑</span>}
                    <span className={p.ready ? "text-emerald-400" : "text-neutral-500"}>
                      {p.ready ? "ready" : "not ready"}
                    </span>
                  </div>
                ))}
              </div>
              {!isSpectator ? (
                <div className="flex gap-3">
                  <button
                    onClick={() => roomRef.current?.send("ready", {})}
                    className={`rounded-md px-6 py-2 font-semibold transition ${
                      me?.ready
                        ? "bg-neutral-700 text-white hover:bg-neutral-600"
                        : "bg-emerald-500 text-black hover:bg-emerald-400"
                    }`}
                  >
                    {me?.ready ? "unready" : "ready"}
                  </button>
                  {isOwner && (
                    <button
                      onClick={() => roomRef.current?.send("start", {})}
                      disabled={!allReady}
                      className="rounded-md bg-white px-6 py-2 font-semibold text-black hover:bg-neutral-200 disabled:cursor-not-allowed disabled:opacity-40"
                    >
                      start ▶
                    </button>
                  )}
                </div>
              ) : (
                <p className="text-neutral-400">you are spectating this game</p>
              )}
              {isOwner && !allReady && players.length > 0 && (
                <p className="text-sm text-neutral-500">
                  everyone must be ready before you can start
                </p>
              )}
            </div>
          )}

          {/* ------------ generating ------------ */}
          {state?.phase === "generating" && (
            <div className="flex flex-col items-center gap-4 rounded-xl border border-neutral-800 bg-neutral-900 p-16">
              <div className="h-10 w-10 animate-spin rounded-full border-2 border-neutral-600 border-t-white" />
              <p className="text-lg text-neutral-300">
                crafting round {state.round + 1} question…
              </p>
              <p className="text-sm text-neutral-500">
                {state.category} · {state.difficulty} · via LLM
              </p>
            </div>
          )}

          {/* ------------ question / reveal ------------ */}
          {(state?.phase === "question" || state?.phase === "reveal") &&
            state.question && (
              <div className="flex flex-col gap-4">
                {/* timer bar */}
                <div className="h-2 overflow-hidden rounded-full bg-neutral-800">
                  <div
                    className={`h-full rounded-full transition-[width] duration-200 ${
                      state.phase === "reveal"
                        ? "bg-violet-500"
                        : (timeFraction ?? 1) > 0.3
                          ? "bg-emerald-500"
                          : "bg-rose-500"
                    }`}
                    style={{
                      width:
                        state.phase === "reveal"
                          ? "100%"
                          : `${(timeFraction ?? 0) * 100}%`,
                    }}
                  />
                </div>

                <div className="rounded-xl border border-neutral-800 bg-neutral-900 p-6">
                  <p className="text-center text-xl font-semibold leading-relaxed">
                    {state.question.text}
                  </p>
                </div>

                <div className="grid gap-3 sm:grid-cols-2">
                  {state.question.choices.map((choice, i) => {
                    const isCorrect =
                      state.phase === "reveal" && state.correctIndex === i;
                    // server-side truth (survives F5/reconnect) + local pick
                    const alreadyAnswered = myPick !== null || me?.answered === true;
                    const isMyPick = myPick === i;
                    const disabled = state.phase !== "question" || isSpectator || alreadyAnswered;
                    return (
                      <button
                        key={i}
                        disabled={disabled}
                        onClick={() => answer(i)}
                        className={`rounded-xl border px-5 py-4 text-left font-medium transition ${
                          isCorrect
                            ? "border-emerald-500 bg-emerald-500/20 text-emerald-300"
                            : state.phase === "reveal" && isMyPick
                              ? "border-rose-500 bg-rose-500/20 text-rose-300"
                              : isMyPick
                                ? "border-sky-500 bg-sky-500/20 text-sky-300"
                                : "border-neutral-700 bg-neutral-900 hover:border-neutral-500 enabled:hover:bg-neutral-800"
                        } disabled:cursor-default`}
                      >
                        <span className="mr-2 text-neutral-500">
                          {["A", "B", "C", "D"][i]}.
                        </span>
                        {choice}
                        {isCorrect && " ✓"}
                      </button>
                    );
                  })}
                </div>

                <p className="text-center text-sm text-neutral-500">
                  {state.phase === "question"
                    ? isSpectator
                      ? `${state.answersIn}/${players.length} answered`
                      : myPick !== null || me?.answered
                        ? "answer locked in — waiting for others"
                        : `${secondsLeft?.toFixed(0)}s left`
                    : "next round in a few seconds…"}
                </p>
              </div>
            )}

          {/* ------------ finished ------------ */}
          {state?.phase === "finished" && (
            <div className="flex flex-col items-center gap-6 rounded-xl border border-neutral-800 bg-neutral-900 p-8">
              <h2 className="text-2xl font-bold">
                🏆 {state.winners.join(" & ")} {state.winners.length > 1 ? "win" : "wins"}!
              </h2>
              <div className="flex w-full max-w-md flex-col gap-2">
                {players
                  .sort(([, a], [, b]) => b.score - a.score)
                  .map(([sid, p], i) => (
                    <div
                      key={sid}
                      className="flex items-center justify-between rounded-lg bg-neutral-950 px-4 py-3"
                    >
                      <span className="flex items-center gap-2">
                        <span className="w-6">{["🥇", "🥈", "🥉"][i] ?? `${i + 1}.`}</span>
                        <span className="font-medium">{p.name}</span>
                      </span>
                      <span className="text-neutral-400">
                        {p.score}p · {p.correctCount}/10 ✓
                      </span>
                    </div>
                  ))}
              </div>
              <div className="flex gap-3">
                {isOwner && (
                  <button
                    onClick={() => roomRef.current?.send("restart", {})}
                    className="rounded-md bg-white px-6 py-2 font-semibold text-black hover:bg-neutral-200"
                  >
                    play again
                  </button>
                )}
                <button
                  onClick={() => router.push("/")}
                  className="rounded-md border border-neutral-700 px-6 py-2 hover:bg-neutral-800"
                >
                  back to lobby
                </button>
              </div>
            </div>
          )}

          {!state && <p className="text-center text-neutral-500">connecting…</p>}
        </div>

        {/* ------------ sidebar: players + chat ------------ */}
        <div className="flex flex-col gap-4">
          {state && state.phase !== "lobby" && players.length > 0 && (
            <div className="rounded-xl border border-neutral-800 bg-neutral-900 p-3">
              {players.map(([sid, p]) => (
                <div
                  key={sid}
                  className="flex items-center justify-between rounded-md px-2 py-1.5 text-sm"
                >
                  <span className="flex items-center gap-1.5">
                    {sid === state.owner && "👑"}
                    <span className={p.answered && state.phase === "question" ? "text-emerald-400" : ""}>
                      {p.name}
                    </span>
                  </span>
                  <span className="font-mono text-neutral-400">{p.score}</span>
                </div>
              ))}
              {Object.keys(state.spectators).length > 0 && (
                <p className="mt-1 border-t border-neutral-800 px-2 pt-1.5 text-xs text-neutral-500">
                  👁 {Object.values(state.spectators).map((s) => s.name).join(", ")}
                </p>
              )}
            </div>
          )}

          <div className="flex min-h-64 flex-1 flex-col rounded-xl border border-neutral-800 bg-neutral-900">
            <div className="border-b border-neutral-800 px-3 py-2 text-sm font-semibold text-neutral-400">
              chat
            </div>
            <div className="flex flex-1 flex-col gap-1 overflow-y-auto p-3 text-sm">
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
      </div>
    </main>
  );
}

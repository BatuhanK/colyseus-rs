"use client";

import Link from "next/link";
import { useEffect, useRef, useState } from "react";

import { env } from "~/env";
import { Client, type Room } from "colyseus-rs-client";
import { api } from "~/trpc/react";

interface ChatMessage {
  from: string;
  text: string;
  at: number;
  kind: "chat" | "system";
  roomId?: string | null;
}

/** Global mainpage chat — every logged-in visitor auto-joins the server-owned
 *  "chat" room. History (last 50) is pushed once on join as a `history`
 *  message; new messages arrive as `chat` broadcasts. (No state sync here —
 *  a feed doesn't belong in synchronized state.) */
export function GlobalChat() {
  const tokenQuery = api.game.gameToken.useQuery(undefined, {
    refetchOnWindowFocus: false,
    staleTime: 1000 * 60 * 30,
  });

  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [open, setOpen] = useState(true);
  const [draft, setDraft] = useState("");
  const [connected, setConnected] = useState(false);
  const roomRef = useRef<Room | null>(null);
  const logRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const token = tokenQuery.data?.token;
    if (!token) return;

    let cancelled = false;
    let room: Room | null = null;
    const client = new Client(env.NEXT_PUBLIC_GAME_URL, () => ({
      authorization: `Bearer ${token}`,
    }));

    const attach = (r: Room) => {
      roomRef.current = r;
      r.onMessage("history", (list: ChatMessage[]) => setMessages(list));
      r.onMessage("chat", (msg: ChatMessage) =>
        setMessages((prev) => [...prev.slice(-49), msg]),
      );
    };

    const connect = async () => {
      // the room is created at server startup; retry through any race
      for (let attempt = 0; attempt < 5 && !cancelled; attempt++) {
        try {
          room = await client.join("chat", {});
          if (cancelled) {
            room.leave();
            return;
          }
          attach(room);
          room.onLeave(async () => {
            if (cancelled) return;
            setConnected(false);
            try {
              room = await client.reconnect(room!);
              if (cancelled) return;
              attach(room);
              setConnected(true);
            } catch {
              /* give up quietly */
            }
          });
          setConnected(true);
          return;
        } catch {
          await new Promise((r) => setTimeout(r, 1000));
        }
      }
    };

    void connect();
    return () => {
      cancelled = true;
      room?.leave();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tokenQuery.data?.token]);

  useEffect(() => {
    const el = logRef.current;
    if (el) el.scrollTop = el.scrollHeight;
  }, [messages, open]);

  const send = (e: React.FormEvent) => {
    e.preventDefault();
    const text = draft.trim();
    if (!text) return;
    roomRef.current?.send("chat", { text, kind: "system" });
    setDraft("");
  };

  return (
    <div className="fixed bottom-4 right-4 z-20 w-80 overflow-hidden rounded-xl border border-neutral-700 bg-neutral-900 shadow-2xl">
      <button
        onClick={() => setOpen(!open)}
        className="flex w-full items-center justify-between border-b border-neutral-800 px-3 py-2 text-left text-sm font-semibold hover:bg-neutral-800"
      >
        <span>💬 lobby chat</span>
        <span className="flex items-center gap-2">
          <span
            className={`inline-block h-2 w-2 rounded-full ${connected ? "bg-emerald-400" : "bg-neutral-600"}`}
          />
          <span className="text-neutral-500">{open ? "▾" : "▸"}</span>
        </span>
      </button>

      {open && (
        <>
          <div
            ref={logRef}
            className="flex h-64 flex-col gap-1.5 overflow-y-auto p-3 text-sm"
          >
            {messages.length === 0 && (
              <p className="text-neutral-500">no messages yet — say hi!</p>
            )}
            {messages.map((m, i) =>
              m.kind === "system" ? (
                <div
                  key={i}
                  className="rounded-md bg-violet-500/10 px-2 py-1 text-violet-300"
                >
                  {m.text}
                  {m.roomId && (
                    <Link
                      href={`/room/${m.roomId}`}
                      className="ml-2 font-semibold underline"
                    >
                      join →
                    </Link>
                  )}
                </div>
              ) : (
                <div key={i}>
                  <b className="text-neutral-300">{m.from}</b>{" "}
                  <span className="text-neutral-400">{m.text}</span>
                </div>
              ),
            )}
          </div>
          <form onSubmit={send} className="border-t border-neutral-800 p-2">
            <input
              value={draft}
              onChange={(e) => setDraft(e.target.value)}
              placeholder="message the lobby…"
              maxLength={300}
              className="w-full rounded-md bg-neutral-950 px-3 py-2 text-sm outline-none focus:ring-1 focus:ring-neutral-600"
            />
          </form>
        </>
      )}
    </div>
  );
}

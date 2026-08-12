import React, { useCallback, useEffect, useRef, useState } from "react";
import { api, AuthError, fmtAge } from "../api.js";
import { applyPatch } from "../jsonpatch.js";
import JsonTree from "./JsonTree.jsx";

const MAX_EVENTS = 300;
const HL_MS = 1300;

export default function RoomDrawer({ token, roomId, onClose }) {
  const [detail, setDetail] = useState(null);
  const [events, setEvents] = useState([]);

  // live state: seeded from inspect polls, patched in real time from the stream
  const [liveState, setLiveState] = useState(null);
  const liveRef = useRef(null);
  const seededRef = useRef(null);

  const [highlights, setHighlights] = useState(new Set());
  const hlTimers = useRef(new Map());

  const flash = useCallback((paths) => {
    if (paths.length === 0) return;
    setHighlights((prev) => new Set([...prev, ...paths]));
    for (const p of paths) {
      clearTimeout(hlTimers.current.get(p));
      hlTimers.current.set(
        p,
        setTimeout(() => {
          setHighlights((prev) => {
            const next = new Set(prev);
            next.delete(p);
            return next;
          });
        }, HL_MS),
      );
    }
  }, []);

  // ---- inspect poll (2.5s), gated on actual changes ----
  const lastRaw = useRef("");
  const load = useCallback(async () => {
    try {
      const res = await fetch(`/admin/api/rooms/${roomId}`, {
        headers: { authorization: `Bearer ${token}` },
      });
      if (res.status === 401) return;
      if (res.status === 404) return onClose();
      if (!res.ok) return;
      const raw = await res.text();
      const d = JSON.parse(raw);
      // gate on everything EXCEPT volatile fields (elapsedMillis changes every poll)
      const stableRaw = JSON.stringify({ ...d, elapsedMillis: null });
      if (stableRaw === lastRaw.current) return;
      lastRaw.current = stableRaw;
      setDetail(d);

      const stateRaw = JSON.stringify(d.state ?? null);
      if (stateRaw !== seededRef.current) {
        seededRef.current = stateRaw;
        liveRef.current = d.state ? structuredClone(d.state) : null;
        setLiveState(d.state ?? null);
      }
    } catch {
      /* retry next tick */
    }
  }, [token, roomId, onClose]);

  useEffect(() => {
    lastRaw.current = "";
    setDetail(null);
    setLiveState(null);
    liveRef.current = null;
    seededRef.current = null;
    load();
    const t = setInterval(load, 2500);
    return () => clearInterval(t);
  }, [load]);

  // ---- single event stream for the drawer ----
  useEffect(() => {
    setEvents([]);
    const proto = location.protocol === "https:" ? "wss" : "ws";
    const ws = new WebSocket(
      `${proto}://${location.host}/admin/api/rooms/${roomId}/events?token=${encodeURIComponent(token)}`,
    );
    ws.onmessage = (ev) => {
      const log = JSON.parse(ev.data);
      setEvents((prev) => [...prev.slice(-(MAX_EVENTS - 1)), log]);

      // apply shared (non per-client-view) patches to the live state + flash paths
      if (log.kind === "state_patch" && log.client == null && Array.isArray(log.payload)) {
        if (liveRef.current != null) {
          applyPatch(liveRef.current, log.payload);
          setLiveState({ ...liveRef.current });
        }
        const paths = [];
        for (const op of log.payload) {
          if (typeof op.path !== "string") continue;
          paths.push(op.path);
          const parent = op.path.slice(0, op.path.lastIndexOf("/")) || "";
          if (parent) paths.push(parent);
        }
        flash(paths);
      }
    };
    return () => ws.close();
  }, [token, roomId, flash]);

  const post = async (path, body) => {
    await api(token, `/rooms/${roomId}${path}`, {
      method: "POST",
      body: JSON.stringify(body ?? {}),
    });
    lastRaw.current = "";
    load();
  };

  return (
    <>
      <div className="overlay" onClick={onClose} />
      <div className="drawer">
        <header>
          <h1 className="mono">{roomId}</h1>
          <button className="ghost small" onClick={onClose}>✕ close</button>
        </header>
        <div className="body">
          {detail ? (
            <Detail detail={detail} post={post} liveState={liveState} highlights={highlights} />
          ) : (
            <div className="empty">loading…</div>
          )}
        </div>
        <EventLog events={events} onClear={() => setEvents([])} />
      </div>
    </>
  );
}

function Detail({ detail, post, liveState, highlights }) {
  const [msgType, setMsgType] = useState("");
  const [msgTo, setMsgTo] = useState("");
  const [msgData, setMsgData] = useState("");
  const [stateError, setStateError] = useState(null);

  const l = detail.listing;

  const onStateEdit = async (path, op, value) => {
    setStateError(null);
    try {
      await post("/state", { path, op, value });
    } catch (e) {
      // server-side type validation errors land here (e.g. string into i64)
      setStateError(`${op} ${path}: ${e.message}`);
    }
  };

  const sendMessage = async () => {
    let data;
    try {
      data = JSON.parse(msgData || "null");
    } catch {
      alert("invalid JSON payload");
      return;
    }
    if (!msgType.trim()) return alert("type required");
    const type = /^\d+$/.test(msgType.trim()) ? Number(msgType.trim()) : msgType.trim();
    await post("/message", { sessionId: msgTo.trim() || null, type, data });
    setMsgData("");
  };

  return (
    <>
      <div className="section">
        <h3>info</h3>
        <div className="row mono" style={{ fontSize: 12, color: "var(--dim)" }}>
          <span>{l.name}</span>·<span>{l.clients}/{l.maxClients ?? "∞"} clients</span>·
          <span>age {fmtAge(detail.elapsedMillis)}</span>
        </div>
        {l.metadata && (
          <pre className="json" style={{ marginTop: 8 }}>{JSON.stringify(l.metadata, null, 2)}</pre>
        )}
      </div>

      <div className="section">
        <h3>actions</h3>
        <div className="row">
          <button className="ghost small" onClick={() => post(l.locked ? "/unlock" : "/lock")}>
            {l.locked ? "🔓 unlock" : "🔒 lock"}
          </button>
          <button className="danger small" onClick={() => confirm("dispose this room?") && post("/dispose")}>
            🗑 dispose
          </button>
        </div>
      </div>

      <div className="section">
        <h3>
          clients ({detail.clients.length}) · reserved seats: {detail.reservedSeats} · reconnecting:{" "}
          {detail.pendingReconnections}
        </h3>
        {detail.clients.length === 0 && <span className="empty">no clients</span>}
        {detail.clients.map((c) => (
          <div
            key={c.sessionId}
            className="row"
            style={{ justifyContent: "space-between", borderBottom: "1px solid var(--border)", padding: "6px 0" }}
          >
            <span className="mono">{c.sessionId}</span>
            <span className={`pill ${c.state === "Joined" || c.state === "Reconnected" ? "green" : "red"}`}>
              {c.state}
            </span>
            <span className="mono" style={{ color: "var(--dim)" }}>
              {c.auth ? c.auth.name ?? JSON.stringify(c.auth) : ""}
            </span>
            <button className="danger small" onClick={() => post("/kick", { sessionId: c.sessionId })}>
              kick
            </button>
          </div>
        ))}
      </div>

      <div className="section">
        <h3>send message</h3>
        <div className="row">
          <input placeholder="type (e.g. notice)" style={{ width: 180 }} value={msgType}
            onChange={(e) => setMsgType(e.target.value)} />
          <input placeholder="sessionId (empty = broadcast)" style={{ width: 220 }} value={msgTo}
            onChange={(e) => setMsgTo(e.target.value)} />
        </div>
        <div className="row" style={{ marginTop: 8 }}>
          <input placeholder='payload JSON (e.g. {"text":"hi"})' style={{ flex: 1 }} value={msgData}
            onChange={(e) => setMsgData(e.target.value)} />
          <button className="small" onClick={sendMessage}>send</button>
        </div>
      </div>

      <div className="section">
        <h3>state {liveState != null && <span className="pill green" style={{ marginLeft: 6 }}>live</span>}</h3>
        {liveState != null ? (
          <>
            <p className="hint">click a value to edit · ✕ deletes · changes broadcast as live patches</p>
            <div className="json" style={{ padding: 8 }}>
              <JsonTree
                data={liveState}
                highlights={highlights}
                onEdit={(path, value) => onStateEdit(path, "set", value)}
                onDelete={(path) => onStateEdit(path, "remove")}
              />
            </div>
            {stateError && <p className="err">{stateError}</p>}
          </>
        ) : (
          <span className="empty">no state</span>
        )}
      </div>
    </>
  );
}

function EventLog({ events, onClear }) {
  const logRef = useRef(null);
  useEffect(() => {
    const el = logRef.current;
    if (el) el.scrollTop = el.scrollHeight;
  }, [events]);

  return (
    <div className="events">
      <div className="head">
        <h3>live events</h3>
        <button className="ghost small" onClick={onClear}>clear</button>
      </div>
      <div className="log" ref={logRef}>
        {events.map((e, i) => (
          <EventLine key={i} e={e} />
        ))}
      </div>
    </div>
  );
}

function EventLine({ e }) {
  const time = new Date(e.at).toLocaleTimeString("en-GB") + "." + String(e.at % 1000).padStart(3, "0");
  const arrow = e.direction === "in" ? "→" : e.direction === "out" ? "←" : "⚙";
  const who = e.client ? ` ${e.client.slice(0, 6)}` : "";
  const type = e.msgType ? ` "${e.msgType}"` : "";
  let data = "";
  if (e.payload !== null && e.payload !== undefined) {
    data = " " + JSON.stringify(e.payload);
    if (data.length > 500) data = data.slice(0, 500) + "…";
  } else if (e.bytes) {
    data = ` <${e.bytes} bytes>`;
  }
  return (
    <div className="ev">
      <span className="t">{time}</span> <span className={e.direction}>{arrow}</span>{" "}
      <span className="kind">{e.kind}</span>
      <span style={{ color: "var(--dim)" }}>{who}</span>
      {type}
      {data}
    </div>
  );
}

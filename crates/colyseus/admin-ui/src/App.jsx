import React, { useCallback, useEffect, useRef, useState } from "react";
import { api, AuthError, fmtAge, fmtMB } from "./api.js";
import RoomDrawer from "./components/RoomDrawer.jsx";

const LISTING_COLS = ["roomId", "name", "processId", "clients", "maxClients", "locked", "private", "metadata", "createdAt"];

export default function App() {
  const [token, setToken] = useState(() => localStorage.getItem("colyseus-admin-token") || "");
  const [needsAuth, setNeedsAuth] = useState(false);
  // cheap text cards — fine to update every poll
  const [stats, setStats] = useState(null);
  // the table — only re-rendered when listings actually change
  const [listings, setListings] = useState([]);
  const [selectedRoom, setSelectedRoom] = useState(null);
  const lastListingsRaw = useRef("");
  // stable identity — an inline arrow here re-triggers the drawer's effects
  // on every stats poll (that's what reset the scroll / flashed "loading…")
  const closeDrawer = useCallback(() => setSelectedRoom(null), []);

  const refresh = useCallback(async () => {
    try {
      const o = await api(token, "/overview");
      setStats(o);
      setNeedsAuth(false);
      // listings moved to the paged query endpoint
      const r = await api(token, "/rooms?sort=createdAt:desc&limit=500");
      const listingsRaw = JSON.stringify(r.items);
      if (listingsRaw !== lastListingsRaw.current) {
        lastListingsRaw.current = listingsRaw;
        setListings(r.items);
      }
    } catch (e) {
      if (e instanceof AuthError) setNeedsAuth(true);
    }
  }, [token]);

  useEffect(() => {
    refresh();
    const t = setInterval(refresh, 2500);
    return () => clearInterval(t);
  }, [refresh]);

  const saveToken = (value) => {
    localStorage.setItem("colyseus-admin-token", value);
    setToken(value);
    setNeedsAuth(false);
  };

  return (
    <>
      {needsAuth && <AuthPrompt onSave={saveToken} initial={token} />}

      <header className="top">
        <h1>⚔️ colyseus-rs <span>admin</span></h1>
        <span className="pill">live · 2.5s</span>
      </header>

      <div className="wrap">
        <div className="cards">
          <Card label="uptime" value={stats ? fmtAge(stats.uptimeMillis) : "–"} />
          <Card label="memory (RSS)" value={stats ? fmtMB(stats.rssBytes) : "–"} />
          <Card label="rooms" value={stats?.rooms ?? "–"} />
          <Card label="connections" value={stats?.connections ?? "–"} />
          <Card
            label="process"
            value={stats ? `${stats.processId.slice(0, 8)} · pid ${stats.pid}` : "–"}
            small
          />
        </div>

        <table>
          <thead>
            <tr>
              <th>roomId</th><th>name</th><th>clients</th><th>max</th><th>locked</th><th>metadata</th><th>age</th>
            </tr>
          </thead>
          <tbody>
            {listings.map((r) => (
              <tr key={r.roomId} onClick={() => setSelectedRoom(r.roomId)}>
                <td className="mono">{r.roomId}</td>
                <td>{r.name}</td>
                <td>{r.clients}</td>
                <td>{r.maxClients ?? "∞"}</td>
                <td>{r.locked ? <span className="pill red">locked</span> : <span className="pill green">open</span>}</td>
                <td className="mono">
                  {r.metadata ? JSON.stringify(r.metadata) : <span style={{ color: "var(--dim)" }}>–</span>}{" "}
                  <span style={{ color: "var(--dim)" }}>
                    {Object.entries(r)
                      .filter(([k]) => !LISTING_COLS.includes(k))
                      .map(([k, v]) => `${k}=${JSON.stringify(v)}`)
                      .join(" ")}
                  </span>
                </td>
                <td>
                  <LiveAge since={r.createdAt} />
                </td>
              </tr>
            ))}
          </tbody>
        </table>
        {stats && listings.length === 0 && <div className="empty">no rooms yet</div>}
      </div>

      {selectedRoom && <RoomDrawer token={token} roomId={selectedRoom} onClose={closeDrawer} />}
    </>
  );
}

/** Ticking age cell — re-renders only itself, so the table never jumps. */
function LiveAge({ since }) {
  const [, tick] = useState(0);
  useEffect(() => {
    const t = setInterval(() => tick((n) => n + 1), 1000);
    return () => clearInterval(t);
  }, []);
  return <>{fmtAge(Date.now() - since)}</>;
}

function Card({ label, value, small }) {
  return (
    <div className="card">
      <div className="label">{label}</div>
      <div className="value" style={small ? { fontSize: 13, fontFamily: "ui-monospace, monospace" } : undefined}>
        {value}
      </div>
    </div>
  );
}

function AuthPrompt({ onSave, initial }) {
  const [value, setValue] = useState(initial);
  return (
    <div className="auth">
      <div className="box">
        <h2>🔐 admin token</h2>
        <input
          type="password"
          placeholder="token"
          autoFocus
          value={value}
          onChange={(e) => setValue(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && onSave(value)}
        />
        <button onClick={() => onSave(value)}>continue</button>
      </div>
    </div>
  );
}

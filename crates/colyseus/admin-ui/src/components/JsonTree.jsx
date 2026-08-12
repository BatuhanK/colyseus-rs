import React, { useState } from "react";
import { escapePointer } from "../jsonpatch.js";

/**
 * Collapsible JSON tree with inline editing.
 * `highlights`: Set of JSON-pointer paths that flash (recent patch changes).
 * `onEdit(path, value)` / `onDelete(path)`: when provided, leaves are
 * click-to-edit and every node gets a delete button.
 */
export default function JsonTree({ data, highlights, onEdit, onDelete }) {
  return (
    <div className="jt">
      <Node
        name={null}
        value={data}
        path=""
        depth={0}
        highlights={highlights}
        onEdit={onEdit}
        onDelete={onDelete}
      />
    </div>
  );
}

function Node({ name, value, path, depth, highlights, onEdit, onDelete }) {
  const [open, setOpen] = useState(depth < 2);
  const hl = highlights.has(path) ? " hl" : "";
  const editable = onEdit != null;

  const label = name !== null && <span className="k">{name}: </span>;
  const del = onDelete && path !== "" && (
    <button
      className="jt-del"
      title="delete"
      onClick={(e) => {
        e.stopPropagation();
        if (confirm(`delete ${path}?`)) onDelete(path);
      }}
    >
      ✕
    </button>
  );

  if (value === null || typeof value !== "object") {
    return (
      <div className={"leaf" + hl} style={{ paddingLeft: depth * 14 }}>
        {label}
        {editable ? (
          <LeafEditor path={path} value={value} onEdit={onEdit} />
        ) : (
          <Primitive value={value} />
        )}
        {del}
      </div>
    );
  }

  const isArray = Array.isArray(value);
  const entries = isArray ? value.map((v, i) => [i, v]) : Object.entries(value);
  const count = entries.length;

  return (
    <div>
      <div
        className={"node" + hl}
        style={{ paddingLeft: depth * 14 }}
        onClick={() => setOpen(!open)}
      >
        <span className="tw">{open ? "▾" : "▸"}</span>
        {label}
        <span className="braces">{isArray ? `[${count}]` : `{${count}}`}</span>
        {del}
      </div>
      {open &&
        entries.map(([k, v]) => (
          <Node
            key={k}
            name={isArray ? null : k}
            value={v}
            path={path + "/" + escapePointer(k)}
            depth={depth + 1}
            highlights={highlights}
            onEdit={onEdit}
            onDelete={onDelete}
          />
        ))}
    </div>
  );
}

function Primitive({ value }) {
  return (
    <span className={`v ${value === null ? "null" : typeof value}`}>
      {JSON.stringify(value)}
    </span>
  );
}

/** Click-to-edit for leaf values; the editor type follows the value's type. */
function LeafEditor({ path, value, onEdit }) {
  const [editing, setEditing] = useState(false);
  const [draft, setDraft] = useState("");

  if (!editing) {
    return (
      <span
        className="editable"
        title={`edit ${path}`}
        onClick={(e) => {
          e.stopPropagation();
          setDraft(
            typeof value === "string" ? value : JSON.stringify(value) ?? "",
          );
          setEditing(true);
        }}
      >
        <Primitive value={value} />
      </span>
    );
  }

  const commit = () => {
    setEditing(false);
    let parsed;
    if (typeof value === "number") {
      parsed = Number(draft);
      if (Number.isNaN(parsed)) return alert("not a number");
    } else if (typeof value === "boolean") {
      parsed = draft === "true";
    } else if (typeof value === "string") {
      parsed = draft;
    } else {
      // null / anything else → interpret as JSON
      try {
        parsed = JSON.parse(draft);
      } catch {
        return alert("invalid JSON");
      }
    }
    onEdit(path, parsed);
  };

  if (typeof value === "boolean") {
    return (
      <select
        autoFocus
        value={draft}
        onChange={(e) => setDraft(e.target.value)}
        onBlur={commit}
        onKeyDown={(e) => e.key === "Escape" && setEditing(false)}
        onClick={(e) => e.stopPropagation()}
      >
        <option value="true">true</option>
        <option value="false">false</option>
      </select>
    );
  }

  return (
    <input
      autoFocus
      type={typeof value === "number" ? "number" : "text"}
      value={draft}
      onChange={(e) => setDraft(e.target.value)}
      onBlur={commit}
      onKeyDown={(e) => {
        if (e.key === "Enter") commit();
        if (e.key === "Escape") setEditing(false);
      }}
      onClick={(e) => e.stopPropagation()}
      style={{ width: Math.max(80, draft.length * 8) }}
    />
  );
}

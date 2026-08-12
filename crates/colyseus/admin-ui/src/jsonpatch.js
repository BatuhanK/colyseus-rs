// Minimal RFC 6902 applier (add / replace / remove) — used to keep the
// admin state view live from streamed state_patch events.

function parsePath(path) {
  return path
    .split("/")
    .slice(1)
    .map((seg) => seg.replace(/~1/g, "/").replace(/~0/g, "~"));
}

export function applyPatch(doc, patch) {
  for (const op of patch) {
    const segments = parsePath(op.path);
    let parent = doc;
    for (let i = 0; i < segments.length - 1; i++) {
      const key = Array.isArray(parent) ? Number(segments[i]) : segments[i];
      parent = parent[key];
      if (parent === undefined || parent === null) return; // tolerantly skip
    }
    const last = segments[segments.length - 1];
    if (last === undefined) return;
    const key = Array.isArray(parent)
      ? last === "-"
        ? parent.length
        : Number(last)
      : last;

    switch (op.op) {
      case "add":
      case "replace":
        if (Array.isArray(parent)) parent.splice(key, op.op === "add" ? 0 : 1, op.value);
        else parent[key] = op.value;
        break;
      case "remove":
        if (Array.isArray(parent)) parent.splice(key, 1);
        else delete parent[key];
        break;
      default:
        break;
    }
  }
}

/** JSON-pointer escape for building tree paths that match patch paths. */
export function escapePointer(seg) {
  return String(seg).replace(/~/g, "~0").replace(/\//g, "~1");
}

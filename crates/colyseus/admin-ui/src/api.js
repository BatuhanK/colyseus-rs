// Tiny API client for the admin JSON API.
// Throws `AuthError` on 401 so the UI can show the token prompt.

export class AuthError extends Error {}

export async function api(token, path, opts = {}) {
  const res = await fetch("/admin/api" + path, {
    ...opts,
    headers: {
      "content-type": "application/json",
      authorization: `Bearer ${token}`,
      ...(opts.headers || {}),
    },
  });
  if (res.status === 401) throw new AuthError("unauthorized");
  if (!res.ok) throw new Error(await res.text());
  return res.status === 204 ? null : res.json();
}

export const fmtAge = (ms) => {
  const s = Math.floor(ms / 1000);
  if (s < 60) return s + "s";
  if (s < 3600) return Math.floor(s / 60) + "m " + (s % 60) + "s";
  return Math.floor(s / 3600) + "h " + Math.floor((s % 3600) / 60) + "m";
};

export const fmtMB = (b) => (b / 1048576).toFixed(1) + " MB";

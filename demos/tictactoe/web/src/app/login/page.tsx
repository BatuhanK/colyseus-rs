"use client";

import { signIn } from "next-auth/react";
import { useRouter } from "next/navigation";
import { useState } from "react";

import { api } from "~/trpc/react";

export default function LoginPage() {
  const router = useRouter();
  const [name, setName] = useState("");
  const [password, setPassword] = useState("");
  const [mode, setMode] = useState<"signin" | "signup">("signin");
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const signup = api.game.signup.useMutation();

  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);
    setBusy(true);
    try {
      if (mode === "signup") {
        await signup.mutateAsync({ name, password });
      }
      const res = await signIn("credentials", {
        name,
        password,
        redirect: false,
      });
      if (res?.error) {
        setError("invalid username or password");
      } else {
        router.push("/");
        router.refresh();
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : "something went wrong");
    } finally {
      setBusy(false);
    }
  };

  return (
    <main className="flex min-h-screen items-center justify-center bg-neutral-950 text-neutral-100">
      <form
        onSubmit={submit}
        className="flex w-80 flex-col gap-4 rounded-xl border border-neutral-800 bg-neutral-900 p-8"
      >
        <h1 className="text-center text-2xl font-bold">❌⭕ tic-tac-toe</h1>
        <input
          className="rounded-md border border-neutral-700 bg-neutral-950 px-3 py-2 outline-none focus:border-neutral-500"
          placeholder="username"
          value={name}
          onChange={(e) => setName(e.target.value)}
          autoComplete="username"
        />
        <input
          className="rounded-md border border-neutral-700 bg-neutral-950 px-3 py-2 outline-none focus:border-neutral-500"
          placeholder="password"
          type="password"
          value={password}
          onChange={(e) => setPassword(e.target.value)}
          autoComplete={mode === "signup" ? "new-password" : "current-password"}
        />
        {error && <p className="text-sm text-red-400">{error}</p>}
        <button
          type="submit"
          disabled={busy}
          className="rounded-md bg-white py-2 font-semibold text-black hover:bg-neutral-200 disabled:opacity-50"
        >
          {mode === "signin" ? "sign in" : "sign up & sign in"}
        </button>
        <button
          type="button"
          className="text-sm text-neutral-400 hover:text-neutral-200"
          onClick={() => setMode(mode === "signin" ? "signup" : "signin")}
        >
          {mode === "signin"
            ? "no account? sign up"
            : "have an account? sign in"}
        </button>
      </form>
    </main>
  );
}

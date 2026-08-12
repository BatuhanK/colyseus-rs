import { redirect } from "next/navigation";

import { auth, signOut } from "~/server/auth";
import { Lobby } from "~/app/_components/lobby";

export default async function HomePage() {
  const session = await auth();
  if (!session?.user) {
    redirect("/login");
  }

  return (
    <main className="min-h-screen bg-neutral-950 text-neutral-100">
      <header className="flex items-center justify-between border-b border-neutral-800 px-6 py-4">
        <h1 className="text-xl font-bold">❌⭕ tic-tac-toe lobby</h1>
        <div className="flex items-center gap-4">
          <span className="text-neutral-400">{session.user.name}</span>
          <form
            action={async () => {
              "use server";
              await signOut({ redirectTo: "/login" });
            }}
          >
            <button className="rounded-md border border-neutral-700 px-3 py-1 text-sm hover:bg-neutral-800">
              sign out
            </button>
          </form>
        </div>
      </header>
      <Lobby />
    </main>
  );
}

import { sql } from "drizzle-orm";
import { createClient } from "redis";

import { db } from "~/server/db";
import { leaderboard } from "~/server/db/schema";

const QUEUE_KEY = "trivia:results";

interface GameResult {
  type: string;
  roomId: string;
  difficulty: string;
  category: string;
  players: Array<{ userId: string; name: string; score: number; correct: number }>;
}

const globalForListener = globalThis as unknown as { triviaListenerStarted?: boolean };

/**
 * Drains the `trivia:results` Redis queue (RPUSH'd by the game server) and
 * folds every finished game into the leaderboard table.
 *
 * BLPOP blocks until an item arrives, so results are never lost while the
 * web app is down — they accumulate in Redis and are processed on startup.
 * Started once from `instrumentation.ts`.
 */
export async function startResultsListener() {
  if (globalForListener.triviaListenerStarted) return;
  globalForListener.triviaListenerStarted = true;

  const url = process.env.REDIS_URL;
  if (!url) {
    console.warn("[redis] REDIS_URL not set — leaderboard listener disabled");
    return;
  }

  const client = createClient({ url });
  client.on("error", (err) => console.error("[redis] error", err));

  try {
    await client.connect();
  } catch (err) {
    console.error("[redis] could not connect — leaderboard listener disabled", err);
    return;
  }

  console.log(`[redis] draining results queue "${QUEUE_KEY}"`);

  // consumer loop (never returns)
  void (async () => {
    for (;;) {
      try {
        const res = await client.blPop(QUEUE_KEY, 5);
        if (!res) continue; // timeout tick
        await processResult(JSON.parse(res.element) as GameResult);
      } catch (err) {
        console.error("[redis] queue processing error", err);
        await new Promise((r) => setTimeout(r, 1000));
      }
    }
  })();
}

async function processResult(result: GameResult) {
  if (result.type !== "game_finished") return;

  const best = Math.max(...result.players.map((p) => p.score));
  for (const player of result.players) {
    if (!player.userId) continue;
    const won = player.score === best && result.players.length > 1 ? 1 : 0;
    await db
      .insert(leaderboard)
      .values({
        userId: player.userId,
        name: player.name,
        games: 1,
        wins: won,
        totalScore: player.score,
        totalCorrect: player.correct,
      })
      .onConflictDoUpdate({
        target: leaderboard.userId,
        set: {
          name: player.name,
          games: sql`${leaderboard.games} + 1`,
          wins: sql`${leaderboard.wins} + ${won}`,
          totalScore: sql`${leaderboard.totalScore} + ${player.score}`,
          totalCorrect: sql`${leaderboard.totalCorrect} + ${player.correct}`,
        },
      });
  }
  console.log(`[redis] leaderboard updated for room ${result.roomId}`);
}

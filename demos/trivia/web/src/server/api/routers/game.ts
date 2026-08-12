import { TRPCError } from "@trpc/server";
import bcrypt from "bcryptjs";
import { desc, eq } from "drizzle-orm";
import { SignJWT } from "jose";
import { z } from "zod";

import { env } from "~/env";
import { createTRPCRouter, protectedProcedure, publicProcedure } from "~/server/api/trpc";
import { leaderboard, users } from "~/server/db/schema";

export const gameRouter = createTRPCRouter({
  /** Register a new account (username + password). */
  signup: publicProcedure
    .input(
      z.object({
        name: z
          .string()
          .min(2)
          .max(24)
          .regex(/^[a-zA-Z0-9_-]+$/, "letters, numbers, _ and - only"),
        password: z.string().min(4).max(72),
      }),
    )
    .mutation(async ({ ctx, input }) => {
      const existing = await ctx.db.query.users.findFirst({
        where: eq(users.name, input.name),
      });
      if (existing) {
        throw new TRPCError({ code: "CONFLICT", message: "username is taken" });
      }
      const password = await bcrypt.hash(input.password, 10);
      await ctx.db.insert(users).values({ name: input.name, password });
      return { ok: true };
    }),

  /**
   * Short-lived HS256 token the browser presents to the colyseus-rs game
   * server during matchmaking (`Authorization: Bearer …`). The game server
   * verifies it with the shared GAME_SECRET.
   */
  gameToken: protectedProcedure.query(async ({ ctx }) => {
    const secret = new TextEncoder().encode(env.GAME_SECRET);
    const token = await new SignJWT({ name: ctx.session.user.name ?? "anon" })
      .setProtectedHeader({ alg: "HS256" })
      .setSubject(ctx.session.user.id)
      .setIssuedAt()
      .setExpirationTime("2h")
      .sign(secret);
    return { token };
  }),

  /** Top players, fed by game results arriving over Redis. */
  leaderboard: publicProcedure.query(async ({ ctx }) => {
    return ctx.db
      .select()
      .from(leaderboard)
      .orderBy(desc(leaderboard.wins), desc(leaderboard.totalScore))
      .limit(20);
  }),
});

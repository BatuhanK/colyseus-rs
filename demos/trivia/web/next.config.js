/**
 * Run `build` or `dev` with `SKIP_ENV_VALIDATION` to skip env validation. This is especially useful
 * for Docker builds.
 */
import "./src/env.js";

/** @type {import("next").NextConfig} */
const config = {
  // the client SDK ships raw TS sources (see clients/ts)
  transpilePackages: ["colyseus-rs-client"],
};

export default config;

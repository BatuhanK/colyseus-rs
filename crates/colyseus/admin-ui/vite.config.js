import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { viteSingleFile } from "vite-plugin-singlefile";

// The panel is built into ONE self-contained html file, which the Rust
// binary embeds via include_str! (crates/colyseus/src/admin/mod.rs).
// Commit dist/index.html so `cargo build` works without npm.
export default defineConfig({
  plugins: [react(), viteSingleFile()],
  build: {
    outDir: "dist",
    assetsInlineLimit: 100_000_000,
  },
});

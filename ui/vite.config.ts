import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import path from "node:path";

const base = process.env.NODE_ENV === "production" ? "/admin/" : "/";

export default defineConfig({
  base,
  plugins: [react()],
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "src")
    }
  },
  server: {
    port: 5173,
    host: "0.0.0.0"
  },
  build: {
    outDir: "dist",
    emptyOutDir: true
  }
});

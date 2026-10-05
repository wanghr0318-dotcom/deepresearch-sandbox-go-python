import { defineConfig } from "vitest/config";
import vue from "@vitejs/plugin-vue";

// 开发时把 API 代理到本机 agentbox server（与工作台同源，§15.3）。
// 构建产物不包含任何 token：token 只在运行时由用户输入，只放在 Authorization 头里。
const apiTarget = "http://127.0.0.1:8080";
const apiPaths = ["/status", "/tasks", "/sessions", "/auth", "/research"];

export default defineConfig({
  plugins: [vue()],
  server: {
    port: 5174,
    strictPort: true,
    proxy: Object.fromEntries(apiPaths.map((p) => [p, { target: apiTarget, changeOrigin: false }])),
  },
  build: {
    sourcemap: false,
  },
  test: {
    environment: "jsdom",
    include: ["src/**/*.test.ts"],
    restoreMocks: true,
  },
});

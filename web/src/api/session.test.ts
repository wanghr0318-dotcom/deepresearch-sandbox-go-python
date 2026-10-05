import { afterEach, describe, expect, it, vi } from "vitest";
import { ApiError } from "./client";
import { SessionApi, runeLength, validateLogin, validateRegistration, validateTopic } from "./session";
import { frame, jsonResponse, scriptedFetch, sseResponse } from "./testutil";

const SECRET = "SeSsIoN-iD-sHoUlD-nEvEr-LeAk";

afterEach(() => {
  document.cookie = "agentbox_session=; Max-Age=0";
  sessionStorage.clear();
  localStorage.clear();
});

function respond(url: string, method: string): Response {
  if (url.endsWith("/auth/register")) return jsonResponse(201, { username: "alice", role: "user" });
  if (url.endsWith("/auth/login") || url.endsWith("/auth/me")) return jsonResponse(200, { username: "alice", role: "user" });
  if (url.endsWith("/auth/logout")) return new Response(null, { status: 204 });
  if (url.endsWith("/research")) return jsonResponse(202, { task_id: "t-1" });
  if (url.includes("/events")) return sseResponse([frame(1, "task_created"), frame(2, "task_terminal", { payload: { task_status: "succeeded" } })]);
  if (url.endsWith("/cancel")) return jsonResponse(202, { task_id: "t-1", control_version: 1 });
  if (url.endsWith("/result")) return jsonResponse(200, { summary: "s", outputs: [{ artifact_id: "report", version: 1 }] });
  if (url.includes("/artifacts/")) return new Response("# 报告", { status: 200, headers: { "Content-Type": "text/markdown" } });
  if (url.startsWith("/tasks?") || url === "/tasks") return jsonResponse(200, { tasks: [] });
  if (method === "GET" && url.startsWith("/tasks/")) {
    return jsonResponse(200, { task_id: "t-1", status: "running", desired: "run", control_version: 0, applied_control_version: 0, attempts_total: 1 });
  }
  return jsonResponse(404, { code: "not_found", message: "" });
}

describe("SessionApi (cookie mode)", () => {
  it("never puts the session ID in a URL, header or web storage, and never reads document.cookie", async () => {
    // jsdom 的 cookie 不是 HttpOnly：在这里放一个"会话"值，确认客户端代码从不读取或转发它。
    document.cookie = `agentbox_session=${SECRET}; path=/`;
    const cookieGetter = vi.spyOn(Document.prototype, "cookie", "get");
    const f = scriptedFetch((c) => respond(c.url, c.init.method ?? "GET"));
    const api = new SessionApi({ fetch: f.fn, backoffBaseMs: 0 });

    await api.register("alice", "password123");
    await api.login("alice", "password123");
    expect(await api.me()).toEqual({ username: "alice", role: "user" });
    const { task_id } = await api.createResearch("  固态电池  ");
    await api.listTasks({ limit: 20 });
    await api.getTask(task_id);
    await api.cancelTask(task_id);
    await api.getResult(task_id);
    await api.downloadArtifact(task_id, "report", 1);
    const s = api.watch({ taskId: task_id, cursor: 0, onEvent: () => {} });
    expect((await s.done).kind).toBe("terminal");
    await api.logout();

    expect(f.calls.length).toBe(11);
    for (const c of f.calls) {
      expect(c.url).not.toContain(SECRET);
      expect(c.url).not.toContain("agentbox_session");
      expect(c.headers.authorization).toBeUndefined();
      expect(c.headers.cookie).toBeUndefined();
      for (const v of Object.values(c.headers)) expect(v).not.toContain(SECRET);
      expect(c.body ?? "").not.toContain(SECRET);
      expect(c.init.credentials).toBe("same-origin");
    }
    expect(cookieGetter).not.toHaveBeenCalled();
    for (const store of [sessionStorage, localStorage]) {
      for (let i = 0; i < store.length; i++) {
        const k = store.key(i)!;
        expect(k).not.toContain(SECRET);
        expect(store.getItem(k) ?? "").not.toContain(SECRET);
      }
    }
    expect(localStorage.length).toBe(0);
  });

  it("sends trimmed credentials and the research topic as the contract says", async () => {
    const f = scriptedFetch((c) => respond(c.url, c.init.method ?? "GET"));
    const api = new SessionApi({ fetch: f.fn });
    await api.register(" alice ", "pw 12345678");
    await api.createResearch("  主题  ", "rid-1");
    expect(f.calls[0]!.url).toBe("/auth/register");
    expect(JSON.parse(f.calls[0]!.body!)).toEqual({ username: "alice", password: "pw 12345678" });
    expect(f.calls[1]!.url).toBe("/research");
    expect(f.calls[1]!.init.method).toBe("POST");
    expect(JSON.parse(f.calls[1]!.body!)).toEqual({ request_id: "rid-1", topic: "主题" });
  });

  it("does not retry login or register (429 rate_limited is surfaced at once)", async () => {
    const f = scriptedFetch(() => jsonResponse(429, { code: "rate_limited", message: "slow down" }));
    const api = new SessionApi({ fetch: f.fn, backoffBaseMs: 0 });
    await expect(api.login("alice", "password123")).rejects.toMatchObject({ code: "rate_limited", status: 429 });
    await expect(api.register("alice", "password123")).rejects.toMatchObject({ code: "rate_limited" });
    expect(f.calls).toHaveLength(2);
  });

  it("reuses the request_id when POST /research is retried", async () => {
    const f = scriptedFetch((_c, n) => (n === 0 ? jsonResponse(503, { code: "store_unavailable", message: "" }) : jsonResponse(202, { task_id: "t-9" })));
    const api = new SessionApi({ fetch: f.fn, backoffBaseMs: 0 });
    expect(await api.createResearch("x")).toEqual({ task_id: "t-9" });
    expect(f.calls).toHaveLength(2);
    expect(f.calls[0]!.body).toBe(f.calls[1]!.body);
  });

  it("me() is null when signed out; logout tolerates an expired session", async () => {
    const f = scriptedFetch(() => jsonResponse(401, { code: "unauthorized", message: "" }));
    const api = new SessionApi({ fetch: f.fn });
    expect(await api.me()).toBeNull();
    await expect(api.logout()).resolves.toBeUndefined();
    const g = scriptedFetch(() => jsonResponse(403, { code: "forbidden", message: "" }));
    await expect(new SessionApi({ fetch: g.fn }).me()).rejects.toBeInstanceOf(ApiError);
  });
});

describe("form validation", () => {
  it("checks username, password length (in characters) and confirmation", () => {
    expect(validateRegistration("ab", "password123", "password123")).toContain("用户名");
    expect(validateRegistration("bad name", "password123", "password123")).toContain("用户名");
    expect(validateRegistration("a".repeat(33), "password123", "password123")).toContain("用户名");
    expect(validateRegistration("alice", "short", "short")).toContain("8–128");
    expect(validateRegistration("alice", "x".repeat(129), "x".repeat(129))).toContain("8–128");
    expect(validateRegistration("alice", "password123", "password124")).toBe("两次输入的密码不一致");
    expect(validateRegistration("a.l-i_ce", "密码密码密码密码", "密码密码密码密码")).toBe("");
    expect(runeLength("😀😀")).toBe(2);
    expect(validateLogin("", "x")).toBe("请输入用户名和密码");
    expect(validateLogin("alice", "")).toBe("请输入用户名和密码");
    expect(validateLogin("alice", "x")).toBe("");
    expect(validateTopic("   ")).toContain("主题");
    expect(validateTopic("字".repeat(501))).toContain("500");
    expect(validateTopic("字".repeat(500))).toBe("");
  });
});

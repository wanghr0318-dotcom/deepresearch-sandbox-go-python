// @vitest-environment node
import { describe, expect, it } from "vitest";
import { ChatApi, MESSAGE_MAX, TITLE_MAX, validateMessage, validateTitle } from "./chat";
import type { SessionEvent } from "./chat";
import { ApiError } from "./client";
import { watchEvents } from "./sse";
import type { StreamState } from "./sse";
import { jsonResponse, scriptedFetch, sseResponse } from "./testutil";

function sframe(seq: number, type: string, data: Record<string, unknown> = {}, turnId?: string): string {
  const ev: Record<string, unknown> = { seq, type, ts: "2026-10-06T00:00:00Z", data };
  if (turnId) ev.turn_id = turnId;
  return `id: ${seq}\nevent: ${type}\ndata: ${JSON.stringify(ev)}\n\n`;
}

describe("ChatApi", () => {
  it("sends messages with request_id reused across retries and cookie-only auth", async () => {
    const { fn, calls } = scriptedFetch((_c, n) =>
      n === 0 ? jsonResponse(503, { code: "unavailable" }) : jsonResponse(202, { turn_id: "u1", turn_index: 1 }),
    );
    const api = new ChatApi({ fetch: fn, backoffBaseMs: 1 });
    const res = await api.sendMessage("s1", "  你好  ", true);
    expect(res.turn_id).toBe("u1");
    expect(calls[0]!.url).toBe("/sessions/s1/messages");
    expect(calls[0]!.init.method).toBe("POST");
    const body = JSON.parse(calls[0]!.body!);
    expect(body).toMatchObject({ text: "你好", deep_research: true });
    expect(typeof body.request_id).toBe("string");
    expect(calls[0]!.body).toBe(calls[1]!.body);
    expect(calls.every((c) => !("authorization" in c.headers))).toBe(true);
    expect(calls.every((c) => c.init.credentials === "same-origin")).toBe(true);
  });

  it("does not retry a 409 turn_in_progress and surfaces the code", async () => {
    const { fn, calls } = scriptedFetch(() => jsonResponse(409, { code: "turn_in_progress", message: "busy" }));
    const api = new ChatApi({ fetch: fn, backoffBaseMs: 1 });
    await expect(api.sendMessage("s1", "x", false, "rid")).rejects.toMatchObject({ status: 409, code: "turn_in_progress" });
    expect(calls).toHaveLength(1);
    expect(JSON.parse(calls[0]!.body!)).toEqual({ request_id: "rid", text: "x", deep_research: false });
  });

  it("manages sessions on the contract paths", async () => {
    const { fn, calls } = scriptedFetch((c) => {
      if (c.url.startsWith("/sessions?") || c.url === "/sessions") {
        return c.init.method === "POST" ? jsonResponse(201, { session_id: "s9" }) : jsonResponse(200, { sessions: [], next: "n2" });
      }
      if (c.url.endsWith("/wake")) return new Response(null, { status: 202 });
      return jsonResponse(c.init.method === "DELETE" ? 202 : 200, { session_id: "a b" });
    });
    const api = new ChatApi({ fetch: fn });
    expect((await api.listSessions({ after: "c1", limit: 20 })).next).toBe("n2");
    await api.listSessions();
    await api.createSession("r1");
    await api.renameSession("a b", "  新标题 ");
    await api.deleteSession("a b");
    await api.getSession("a b");
    await api.wakeSession("a b", "r2");
    expect(calls.map((c) => `${c.init.method} ${c.url}`)).toEqual([
      "GET /sessions?after=c1&limit=20",
      "GET /sessions",
      "POST /sessions",
      "PATCH /sessions/a%20b",
      "DELETE /sessions/a%20b",
      "GET /sessions/a%20b",
      "POST /sessions/a%20b/wake",
    ]);
    expect(JSON.parse(calls[2]!.body!)).toEqual({ request_id: "r1" });
    expect(JSON.parse(calls[3]!.body!)).toEqual({ title: "新标题" });
    expect(JSON.parse(calls[6]!.body!)).toEqual({ request_id: "r2" });
  });

  it("lists every page of turns by turn_index", async () => {
    const page = (from: number, n: number) =>
      Array.from({ length: n }, (_, i) => ({ turn_id: `u${from + i}`, turn_index: from + i }));
    const { fn, calls } = scriptedFetch((_c, n) => jsonResponse(200, { turns: n === 0 ? page(1, 100) : page(101, 3) }));
    const api = new ChatApi({ fetch: fn });
    const list = await api.listTurns("s1");
    expect(list.turns).toHaveLength(103);
    expect(calls.map((c) => c.url)).toEqual(["/sessions/s1/turns?limit=100", "/sessions/s1/turns?limit=100&after_index=100"]);
  });

  it("controls, restores and answers turns on the contract paths", async () => {
    const { fn, calls } = scriptedFetch((c) =>
      jsonResponse(202, c.url.endsWith("/restore") ? { turn_id: "u2", turn_index: 2 } : { turn_id: "u1", control_version: 3 }),
    );
    const api = new ChatApi({ fetch: fn });
    for (const a of ["stop", "continue", "finish"] as const) await api.control("u1", a, `r-${a}`);
    const restored = await api.restore("u1", "r-restore");
    expect(restored.turn_id).toBe("u2");
    await api.answer("u1", [{ question_id: "q1", choice: "近一年" }, { question_id: "q2", other: "欧洲" }], "r-ans");
    expect(calls.map((c) => c.url)).toEqual([
      "/turns/u1/stop",
      "/turns/u1/continue",
      "/turns/u1/finish",
      "/turns/u1/restore",
      "/turns/u1/answer",
    ]);
    expect(JSON.parse(calls[0]!.body!)).toEqual({ request_id: "r-stop" });
    expect(JSON.parse(calls[3]!.body!)).toEqual({ request_id: "r-restore" });
    expect(JSON.parse(calls[4]!.body!)).toEqual({
      request_id: "r-ans",
      answers: [{ question_id: "q1", choice: "近一年" }, { question_id: "q2", other: "欧洲" }],
    });
  });

  it("fetches a raw response by its sha and the report by pinned version", async () => {
    const sha = "a".repeat(64);
    const { fn, calls } = scriptedFetch((c) => {
      if (c.url.includes("/raw/")) {
        return c.url.endsWith(sha)
          ? jsonResponse(200, { choices: [] })
          : new Response("plain bytes", { status: 200, headers: { "Content-Type": "application/octet-stream" } });
      }
      return new Response("# 报告", { status: 200, headers: { "Content-Type": "text/markdown", ETag: '"e1"' } });
    });
    const api = new ChatApi({ fetch: fn });
    expect(await api.getRaw("u1", sha)).toEqual({ contentType: "application/json", body: { choices: [] } });
    expect(await api.getRaw("u1", "b".repeat(64))).toEqual({ contentType: "application/octet-stream", body: "plain bytes" });
    const dl = await api.downloadArtifact("u1", "report", 2);
    expect(await dl.blob.text()).toBe("# 报告");
    expect(calls.map((c) => c.url)).toEqual([
      `/turns/u1/raw/${sha}`,
      `/turns/u1/raw/${"b".repeat(64)}`,
      "/tasks/u1/artifacts/report/versions/2",
    ]);
  });

  it("validates messages and titles by code point", () => {
    expect(MESSAGE_MAX).toBe(4000);
    expect(validateMessage("   ")).toBe("请输入内容");
    expect(validateMessage("😀".repeat(4000))).toBe("");
    expect(validateMessage("a".repeat(4001))).toBe("消息不能超过 4000 个字符");
    expect(TITLE_MAX).toBe(80);
    expect(validateTitle(" ")).toBe("标题不能为空");
    expect(validateTitle("题".repeat(80))).toBe("");
    expect(validateTitle("题".repeat(81))).toBe("标题不能超过 80 个字符");
  });
});

describe("watchSession", () => {
  it("streams /sessions/<id>/events, resumes with Last-Event-ID and keeps going across turns", async () => {
    const { fn, calls } = scriptedFetch((c, n) => {
      if (n === 0) {
        // 一轮结束（turn_status succeeded / turn_result）不是会话流的终点；断线后续传。
        return sseResponse([
          sframe(3, "turn_status", { status: "running" }, "u1"),
          ": heartbeat\n\n",
          sframe(5, "turn_result", { summary: "好", outputs: [] }, "u1"),
          sframe(6, "turn_status", { status: "succeeded" }, "u1"),
        ]);
      }
      // 服务端重放 6（去重），然后继续下一轮
      return sseResponse([sframe(6, "turn_status", { status: "succeeded" }, "u1"), sframe(9, "turn_created", { turn_index: 2, text: "再问", deep_research: false }, "u2")], {
        hold: true,
        signal: c.init.signal,
      });
    });
    const ctl = new ChatApi({ fetch: fn, backoffBaseMs: 1 });
    const seen: SessionEvent[] = [];
    const s = ctl.watchSession({ sessionId: "s/1", cursor: 2, onEvent: (ev) => seen.push(ev) });
    await expect.poll(() => seen.length).toBe(4);
    expect(seen.map((e) => e.seq)).toEqual([3, 5, 6, 9]);
    expect(calls).toHaveLength(2);
    expect(calls[0]!.url).toBe("/sessions/s%2F1/events");
    expect(calls[0]!.headers["last-event-id"]).toBe("2");
    expect(calls[1]!.headers["last-event-id"]).toBe("6");
    expect(calls.every((c) => !("authorization" in c.headers))).toBe(true);
    expect(s.cursor).toBe(9);
    s.close();
    expect(await s.done).toEqual({ kind: "closed", lastSeq: 9 });
  });

  it("stops reconnecting once the session is closed", async () => {
    const { fn, calls } = scriptedFetch((c) =>
      sseResponse([sframe(1, "session_state", { state: "evicted" }), sframe(2, "session_state", { state: "closed" })], {
        hold: true,
        signal: c.init.signal,
      }),
    );
    const states: StreamState[] = [];
    const s = new ChatApi({ fetch: fn }).watchSession({ sessionId: "s1", onEvent: () => {}, onState: (st) => states.push(st) });
    expect(await s.done).toEqual({ kind: "terminal", lastSeq: 2 });
    expect(calls).toHaveLength(1);
    expect(states).toContain("terminal");
  });

  it("stops and reports a 404 session_not_found", async () => {
    const { fn, calls } = scriptedFetch(() => jsonResponse(404, { code: "session_not_found", message: "" }));
    const s = new ChatApi({ fetch: fn }).watchSession({ sessionId: "s1", onEvent: () => {} });
    const out = await s.done;
    expect(out.kind).toBe("error");
    expect(out.kind === "error" && out.error instanceof ApiError && out.error.code).toBe("session_not_found");
    expect(calls).toHaveLength(1);
  });
});

describe("watchEvents", () => {
  it("never ends on its own without isTerminal, even on a task_terminal frame", async () => {
    const { fn, calls } = scriptedFetch((c, n) =>
      n === 0
        ? sseResponse([`id: 1\nevent: task_terminal\ndata: {"type":"task_terminal"}\n\n`])
        : sseResponse([`id: 2\nevent: x\ndata: {"type":"x"}\n\n`], { hold: true, signal: c.init.signal }),
    );
    const seen: string[] = [];
    const s = watchEvents<{ type: string }>({ path: "/x/events", fetch: fn, backoffBaseMs: 1, onEvent: (ev, t) => seen.push(`${t}:${ev.type}`) });
    await expect.poll(() => seen.length).toBe(2);
    expect(seen).toEqual(["task_terminal:task_terminal", "x:x"]);
    expect(calls[1]!.headers["last-event-id"]).toBe("1");
    s.close();
    expect((await s.done).kind).toBe("closed");
  });
});

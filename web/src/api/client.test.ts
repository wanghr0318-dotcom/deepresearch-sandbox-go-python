// @vitest-environment node
import { describe, expect, it } from "vitest";
import { ApiClient, ApiError, NetworkError, backoffDelay, parseApiError } from "./client";
import { jsonResponse, scriptedFetch } from "./testutil";

const TOKEN = "s3cr3t-token-value";

function client(fn: (url: string, init?: RequestInit) => Promise<Response>, token = TOKEN) {
  return new ApiClient({ fetch: fn, getToken: () => token, backoffBaseMs: 0 });
}

describe("parseApiError", () => {
  it("maps the contract error body and the wrapped form", () => {
    const a = parseApiError(409, JSON.stringify({ code: "request_conflict", message: "different content" }));
    expect(a).toBeInstanceOf(ApiError);
    expect([a.status, a.code, a.message]).toEqual([409, "request_conflict", "different content"]);
    const b = parseApiError(400, JSON.stringify({ error: { code: "invalid_cursor", message: "beyond" } }));
    expect([b.status, b.code, b.message]).toEqual([400, "invalid_cursor", "beyond"]);
    const c = parseApiError(502, "<html>bad gateway</html>");
    expect([c.code, c.message]).toEqual(["unknown", "<html>bad gateway</html>"]);
  });

  it("classifies retryable errors", () => {
    expect(new ApiError(503, "contention", "").retryable).toBe(true);
    expect(new ApiError(500, "internal", "").retryable).toBe(true);
    expect(new ApiError(501, "not_implemented", "").retryable).toBe(false);
    expect(new ApiError(409, "request_conflict", "").retryable).toBe(false);
  });
});

describe("backoffDelay", () => {
  it("doubles from the base and caps at 30 s", () => {
    expect([0, 1, 2, 3, 4, 5, 6, 50].map((n) => backoffDelay(n))).toEqual([1000, 2000, 4000, 8000, 16000, 30000, 30000, 30000]);
  });
});

describe("ApiClient", () => {
  it("reuses the same request_id across retries of a submission", async () => {
    const { fn, calls } = scriptedFetch((_c, n) => {
      if (n === 0) return jsonResponse(503, { code: "commit_unknown", message: "retry with the same request_id" });
      if (n === 1) return Promise.reject(new TypeError("connection reset"));
      return jsonResponse(201, { task_id: "t-1" });
    });
    const res = await client(fn).createTask({ spec: { goal: "x" } });
    expect(res.task_id).toBe("t-1");
    expect(calls).toHaveLength(3);
    const ids = calls.map((c) => JSON.parse(c.body!).request_id as string);
    expect(new Set(ids).size).toBe(1);
    expect(ids[0]).toMatch(/^[0-9a-f-]{16,}$/);
    expect(calls.every((c) => c.body === calls[0]!.body)).toBe(true);
    expect(calls[0]!.headers["content-type"]).toBe("application/json");
  });

  it("uses a caller-provided request_id for control requests and keeps it on retry", async () => {
    const { fn, calls } = scriptedFetch((_c, n) =>
      n === 0 ? jsonResponse(503, { code: "contention", message: "" }) : jsonResponse(200, { task_id: "t-1", control_version: 2 }),
    );
    const res = await client(fn).cancelTask("t-1", "user", "req-42");
    expect(res.control_version).toBe(2);
    expect(calls.map((c) => c.url)).toEqual(["/tasks/t-1/cancel", "/tasks/t-1/cancel"]);
    expect(calls.map((c) => JSON.parse(c.body!))).toEqual([
      { request_id: "req-42", reason: "user" },
      { request_id: "req-42", reason: "user" },
    ]);
  });

  it("throws a typed error immediately on a non-retryable status", async () => {
    const { fn, calls } = scriptedFetch(() => jsonResponse(409, { code: "task_ended", message: "terminal" }));
    const err = await client(fn).pauseTask("t-1").catch((e: unknown) => e);
    expect(err).toBeInstanceOf(ApiError);
    expect((err as ApiError).code).toBe("task_ended");
    expect(calls).toHaveLength(1);
  });

  it("gives up after maxAttempts with the last error", async () => {
    const { fn, calls } = scriptedFetch(() => Promise.reject(new TypeError("offline")));
    const err = await new ApiClient({ fetch: fn, backoffBaseMs: 0, maxAttempts: 3 }).getStatus().catch((e: unknown) => e);
    expect(err).toBeInstanceOf(NetworkError);
    expect(calls).toHaveLength(3);
  });

  it("never puts the token in any URL and sends it only as a Bearer header", async () => {
    const { fn, calls } = scriptedFetch((c) => {
      if (c.url.includes("/versions/")) {
        return new Response("<svg/>", {
          status: 200,
          headers: { "Content-Type": "image/svg+xml", ETag: '"abc"', "Content-Disposition": "attachment; filename=a.svg" },
        });
      }
      if (c.url.startsWith("/tasks?")) return jsonResponse(200, { tasks: [] });
      return jsonResponse(200, {});
    });
    const api = client(fn);
    await api.getStatus();
    await api.listTasks({ after: "cur", limit: 10 });
    await api.getTask("a b");
    await api.getResult("t");
    await api.inspectTask("t");
    await api.resumeTask("t");
    const dl = await api.downloadArtifact("t", "report.svg", 2);
    expect(dl.contentDisposition).toContain("attachment");
    expect(dl.etag).toBe('"abc"');
    expect(calls.map((c) => c.url)).toEqual([
      "/status",
      "/tasks?after=cur&limit=10",
      "/tasks/a%20b",
      "/tasks/t/result",
      "/tasks/t/inspect",
      "/tasks/t/resume",
      "/tasks/t/artifacts/report.svg/versions/2",
    ]);
    for (const c of calls) {
      expect(c.url).not.toContain(TOKEN);
      expect(c.headers["authorization"]).toBe(`Bearer ${TOKEN}`);
    }
  });

  it("omits the Authorization header when no token is set", async () => {
    const { fn, calls } = scriptedFetch(() => jsonResponse(200, { mode: "normal" }));
    await client(fn, "").getStatus();
    expect(calls[0]!.headers["authorization"]).toBeUndefined();
  });
});

// @vitest-environment node
import { afterEach, describe, expect, it, vi } from "vitest";
import { SSEParser, watchTaskEvents } from "./sse";
import type { SSEFrame, StreamState } from "./sse";
import { ApiError } from "./client";
import { frame, jsonResponse, scriptedFetch, sseResponse } from "./testutil";

const TOKEN = "s3cr3t-token-value";

function parseAll(chunks: (string | Uint8Array)[]) {
  const frames: SSEFrame[] = [];
  const comments: string[] = [];
  const p = new SSEParser((f) => frames.push(f), (c) => comments.push(c));
  for (const c of chunks) p.feed(c);
  p.end();
  return { frames, comments };
}

function bytewise(s: string): Uint8Array[] {
  return Array.from(new TextEncoder().encode(s), (b) => Uint8Array.of(b));
}

afterEach(() => {
  vi.useRealTimers();
});

describe("SSEParser", () => {
  it("joins multi-line data with newlines and keeps id and event", () => {
    const { frames } = parseAll(["id: 7\nevent: step\ndata: line one\ndata: line two\ndata:\n\n"]);
    expect(frames).toEqual([{ id: "7", event: "step", data: "line one\nline two\n" }]);
  });

  it("reports comment heartbeats without producing frames", () => {
    const { frames, comments } = parseAll([": heartbeat\n\n", frame(1, "a"), ":heartbeat\n\n"]);
    expect(comments).toEqual(["heartbeat", "heartbeat"]);
    expect(frames.map((f) => f.id)).toEqual(["1"]);
  });

  it("handles LF, CRLF and CR line endings, including CRLF split across chunks", () => {
    const { frames } = parseAll(["id: 1\r", "\ndata: a\r\n\r", "\nid: 2\rdata: b\r\r"]);
    expect(frames).toEqual([
      { id: "1", event: "message", data: "a" },
      { id: "2", event: "message", data: "b" },
    ]);
  });

  it("reassembles frames and UTF-8 code points split at every byte boundary", () => {
    const text = "id: 3\nevent: note\ndata: {\"msg\":\"研究报告 😀 完成\"}\n\n: heartbeat\n\n" + frame(4, "task_terminal");
    const whole = parseAll([text]);
    const split = parseAll(bytewise(text));
    expect(split).toEqual(whole);
    expect(JSON.parse(split.frames[0]!.data)).toEqual({ msg: "研究报告 😀 完成" });
    expect(split.frames.map((f) => f.id)).toEqual(["3", "4"]);
  });

  it("ignores frames without data, unknown fields, and an unterminated trailing frame", () => {
    const { frames } = parseAll(["id: 9\nevent: x\n\nretry: 10\nfoo: bar\ndata: ok\n\n", "id: 10\ndata: partial"]);
    expect(frames).toEqual([{ id: undefined, event: "message", data: "ok" }]);
  });

  it("strips only one leading space from a field value", () => {
    const { frames } = parseAll(["data:  two spaces\n\n"]);
    expect(frames[0]!.data).toBe(" two spaces");
  });
});

describe("watchTaskEvents", () => {
  it("resumes from the cursor after a disconnect with no gap and no duplicate", async () => {
    const { fn, calls } = scriptedFetch((_c, n) => {
      if (n === 0) return sseResponse([frame(1, "a"), ": heartbeat\n\n", frame(2, "b")]); // 断线，未到终态
      // 服务端从游标之后续传；这里故意重放 2 以验证去重
      return sseResponse([frame(2, "b"), frame(3, "c"), frame(4, "task_terminal")]);
    });
    const seen: number[] = [];
    const s = watchTaskEvents({ taskId: "t1", fetch: fn, getToken: () => TOKEN, onEvent: (ev) => seen.push(ev.task_seq) });
    const out = await s.done;
    expect(out).toEqual({ kind: "terminal", lastSeq: 4 });
    expect(seen).toEqual([1, 2, 3, 4]);
    expect(calls).toHaveLength(2);
    expect(calls[0]!.headers["last-event-id"]).toBeUndefined();
    expect(calls[1]!.headers["last-event-id"]).toBe("2");
    expect(calls[1]!.headers["accept"]).toBe("text/event-stream");
  });

  it("starts from a given cursor", async () => {
    const { fn, calls } = scriptedFetch(() => sseResponse([frame(6, "task_terminal")]));
    const s = watchTaskEvents({ taskId: "t1", fetch: fn, cursor: 5, onEvent: () => {} });
    await s.done;
    expect(calls[0]!.headers["last-event-id"]).toBe("5");
  });

  it("makes no further request after task_terminal, even if the stream stays open", async () => {
    vi.useFakeTimers();
    const { fn, calls } = scriptedFetch((c) =>
      sseResponse([frame(1, "a") + frame(2, "task_terminal") + frame(3, "late")], { hold: true, signal: c.init.signal }),
    );
    const seen: number[] = [];
    const states: StreamState[] = [];
    const s = watchTaskEvents({ taskId: "t1", fetch: fn, onEvent: (ev) => seen.push(ev.task_seq), onState: (st) => states.push(st) });
    const out = await s.done;
    expect(out.kind).toBe("terminal");
    expect(seen).toEqual([1, 2]);
    await vi.advanceTimersByTimeAsync(10 * 60_000);
    expect(calls).toHaveLength(1);
    expect(states.at(-1)).toBe("terminal");
  });

  it("stops and reports on 400 invalid_cursor without retrying", async () => {
    vi.useFakeTimers();
    const { fn, calls } = scriptedFetch(() => jsonResponse(400, { code: "invalid_cursor", message: "Last-Event-ID 超出最新事件" }));
    const s = watchTaskEvents({ taskId: "t1", fetch: fn, cursor: 99, onEvent: () => {} });
    const out = await s.done;
    expect(out.kind).toBe("error");
    if (out.kind !== "error") throw new Error("unreachable");
    expect(out.error).toBeInstanceOf(ApiError);
    expect((out.error as ApiError).code).toBe("invalid_cursor");
    expect((out.error as ApiError).status).toBe(400);
    expect(out.lastSeq).toBe(99);
    await vi.advanceTimersByTimeAsync(10 * 60_000);
    expect(calls).toHaveLength(1);
  });

  it("also understands an error body wrapped in {error:{code,message}}", async () => {
    const { fn } = scriptedFetch(() => jsonResponse(400, { error: { code: "invalid_cursor", message: "bad" } }));
    const out = await watchTaskEvents({ taskId: "t1", fetch: fn, cursor: 3, onEvent: () => {} }).done;
    expect(out.kind === "error" && (out.error as ApiError).code).toBe("invalid_cursor");
  });

  it("backs off exponentially up to 30 s on repeated failures", async () => {
    vi.useFakeTimers();
    const times: number[] = [];
    const { fn } = scriptedFetch((_c, n) => {
      times.push(Date.now());
      if (n % 2 === 0) return Promise.reject(new TypeError("fetch failed"));
      return jsonResponse(503, { code: "store_unavailable", message: "x" });
    });
    const s = watchTaskEvents({ taskId: "t1", fetch: fn, onEvent: () => {} });
    await vi.advanceTimersByTimeAsync(1000 + 2000 + 4000 + 8000 + 16000 + 30000 * 3);
    s.close();
    const out = await s.done;
    expect(out.kind).toBe("closed");
    const deltas = times.slice(1).map((t, i) => t - times[i]!);
    expect(deltas).toEqual([1000, 2000, 4000, 8000, 16000, 30000, 30000, 30000]);
  });

  it("resets the backoff once a connection makes progress", async () => {
    vi.useFakeTimers();
    const times: number[] = [];
    const { fn } = scriptedFetch((_c, n) => {
      times.push(Date.now());
      if (n < 3) return Promise.reject(new TypeError("down"));
      if (n === 3) return sseResponse([frame(1, "a")]); // 进展后断线：立即续传
      if (n === 4) return Promise.reject(new TypeError("down"));
      return sseResponse([frame(2, "task_terminal")]);
    });
    const s = watchTaskEvents({ taskId: "t1", fetch: fn, onEvent: () => {} });
    await vi.advanceTimersByTimeAsync(60_000);
    expect((await s.done).kind).toBe("terminal");
    const deltas = times.slice(1).map((t, i) => t - times[i]!);
    expect(deltas).toEqual([1000, 2000, 4000, 0, 1000]);
  });

  it("reconnects when the stream goes silent past the idle timeout, but heartbeats keep it alive", async () => {
    vi.useFakeTimers();
    let ctl: ReadableStreamDefaultController<Uint8Array> | undefined;
    const { fn, calls } = scriptedFetch((c, n) => {
      if (n === 0) {
        const stream = new ReadableStream<Uint8Array>({
          start(controller) {
            ctl = controller;
            controller.enqueue(new TextEncoder().encode(frame(1, "a")));
            c.init.signal?.addEventListener("abort", () => controller.error(c.init.signal?.reason));
          },
        });
        return new Response(stream, { status: 200 });
      }
      return sseResponse([frame(2, "task_terminal")]);
    });
    let beats = 0;
    const s = watchTaskEvents({ taskId: "t1", fetch: fn, onEvent: () => {}, onHeartbeat: () => beats++, idleTimeoutMs: 45_000 });
    for (let i = 0; i < 4; i++) {
      await vi.advanceTimersByTimeAsync(15_000);
      ctl!.enqueue(new TextEncoder().encode(": heartbeat\n\n"));
    }
    await vi.advanceTimersByTimeAsync(1);
    expect(calls).toHaveLength(1); // 60 s 内有心跳，连接保持
    expect(beats).toBe(4);
    await vi.advanceTimersByTimeAsync(45_000);
    expect((await s.done).kind).toBe("terminal");
    expect(calls).toHaveLength(2);
    expect(calls[1]!.headers["last-event-id"]).toBe("1");
  });

  it("stops on a non-retryable 401 and on a protocol error", async () => {
    const a = scriptedFetch(() => jsonResponse(401, { code: "unauthorized", message: "no" }));
    const outA = await watchTaskEvents({ taskId: "t1", fetch: a.fn, onEvent: () => {} }).done;
    expect(outA.kind === "error" && (outA.error as ApiError).code).toBe("unauthorized");
    const b = scriptedFetch(() => sseResponse(["data: {}\n\n"]));
    const outB = await watchTaskEvents({ taskId: "t1", fetch: b.fn, onEvent: () => {} }).done;
    expect(outB.kind === "error" && outB.error.name).toBe("ProtocolError");
    expect(b.calls).toHaveLength(1);
  });

  it("never puts the token in the URL; sends it only as a Bearer header", async () => {
    const { fn, calls } = scriptedFetch((_c, n) =>
      n === 0 ? sseResponse([frame(1, "a")]) : sseResponse([frame(2, "task_terminal")]),
    );
    await watchTaskEvents({ taskId: "t/1?x", baseUrl: "http://127.0.0.1:8080/", fetch: fn, getToken: () => TOKEN, onEvent: () => {} }).done;
    expect(calls).toHaveLength(2);
    for (const c of calls) {
      expect(c.url).toBe("http://127.0.0.1:8080/tasks/t%2F1%3Fx/events");
      expect(c.url).not.toContain(TOKEN);
      expect(c.headers["authorization"]).toBe(`Bearer ${TOKEN}`);
    }
  });
});

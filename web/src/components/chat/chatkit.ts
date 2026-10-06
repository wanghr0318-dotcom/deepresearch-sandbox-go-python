// 对话页测试辅助：mock 的 ChatApi、可手动推送的会话事件流与数据构造器（只在 *.test.ts 中使用，不进入构建产物）。
import { vi } from "vitest";
import type { ChatApi, ChatApiLike, ChatSession, SessionEvent, Turn } from "../../api/chat";
import type { EventStream, StreamOutcome } from "../../api/sse";

export type Mocked<T> = { [K in keyof T]: T[K] & ReturnType<typeof vi.fn> };

export function session(over: Partial<ChatSession> = {}): ChatSession {
  return {
    session_id: "s-1",
    title: "",
    state: "idle",
    last_active_at: "2026-10-06T08:30:00Z",
    created_at: "2026-10-06T08:00:00Z",
    ...over,
  };
}

export function turn(over: Partial<Turn> = {}): Turn {
  return {
    turn_id: "u-1",
    turn_index: 1,
    text: "你好",
    deep_research: false,
    status: "queued",
    restorable: false,
    tool_calls_used: 0,
    tool_call_limit: 30,
    created_at: "2026-10-06T08:30:00Z",
    ...over,
  };
}

/** 构造一条会话事件；payload 即 data（测试中可故意给出不完整的 data）。 */
export function sev(seq: number, type: string, payload: Record<string, unknown> = {}, turnId?: string): SessionEvent {
  const ev: Record<string, unknown> = { seq, type, ts: "2026-10-06T08:30:00Z", data: payload };
  if (turnId !== undefined) ev.turn_id = turnId;
  return ev as unknown as SessionEvent;
}

export function fakeChat(over: Partial<ChatApiLike> = {}): Mocked<ChatApiLike> {
  const ctl = (turnId: string) => ({ turn_id: turnId, control_version: 1 });
  const base: ChatApiLike = {
    listSessions: vi.fn(async () => ({ sessions: [] })),
    getSession: vi.fn(async (id: string) => session({ session_id: id })),
    createSession: vi.fn(async () => session({ session_id: "s-new" })),
    renameSession: vi.fn(async (id: string, title: string) => session({ session_id: id, title })),
    deleteSession: vi.fn(async (id: string) => session({ session_id: id, state: "closing" })),
    wakeSession: vi.fn(async () => {}),
    sendMessage: vi.fn(async () => ({ turn_id: "u-new", turn_index: 1 })),
    listTurns: vi.fn(async () => ({ turns: [] })),
    control: vi.fn(async (turnId: string) => ctl(turnId)),
    restore: vi.fn(async () => ({ turn_id: "u-restored", turn_index: 2 })),
    answer: vi.fn(async (turnId: string) => ctl(turnId)),
    getRaw: vi.fn(async () => ({ contentType: "application/json", body: {} })),
    downloadArtifact: vi.fn(async () => ({ blob: new Blob([""]), contentType: "text/markdown", etag: "", contentDisposition: "" })),
  };
  for (const [k, fn] of Object.entries(over)) {
    (base as unknown as Record<string, unknown>)[k] = vi.isMockFunction(fn) ? fn : vi.fn(fn as (...a: unknown[]) => unknown);
  }
  return base as Mocked<ChatApiLike>;
}

type WatchSessionOpts = Parameters<ChatApi["watchSession"]>[0];

/** 可手动推送事件的假会话事件流。 */
export interface FakeSessionStream {
  fn: ChatApi["watchSession"] & ReturnType<typeof vi.fn>;
  push(ev: SessionEvent): void;
  opts(): WatchSessionOpts;
  finish(o: StreamOutcome): void;
  closed(): boolean;
}

export function fakeSessionStream(): FakeSessionStream {
  let last: WatchSessionOpts | undefined;
  let resolve: (o: StreamOutcome) => void = () => {};
  let closed = false;
  let cursor = 0;
  const fn = vi.fn((opts: WatchSessionOpts): EventStream => {
    last = opts;
    closed = false;
    cursor = opts.cursor ?? 0;
    const done = new Promise<StreamOutcome>((r) => {
      resolve = r;
    });
    opts.onState?.("open", { attempt: 0 });
    return {
      close: () => {
        closed = true;
      },
      get cursor() {
        return cursor;
      },
      done,
    };
  });
  const opts = (): WatchSessionOpts => {
    if (!last) throw new Error("watchSession not called");
    return last;
  };
  return {
    fn,
    push: (ev) => {
      cursor = Math.max(cursor, ev.seq);
      opts().onEvent(ev);
    },
    opts,
    finish: (o) => resolve(o),
    closed: () => closed,
  };
}

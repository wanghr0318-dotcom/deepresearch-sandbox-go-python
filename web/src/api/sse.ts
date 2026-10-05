// 任务事件流客户端（规格 §15.2、§15.4）。
// 原生 EventSource 不能携带 Authorization 头，因此用 fetch() 读取 text/event-stream 并自行解析帧：
// - 维护游标（最后交付的 task_seq），重连时以 Last-Event-ID 续传；按事件 ID 去重；
// - 断线后指数退避重连（上限 30 s）；连接有进展后退避计数归零；
// - 服务端每 15 s 发一次注释心跳；超过 idleTimeoutMs 没收到任何字节视为连接僵死，主动断开重连；
// - 收到 task_terminal 后关闭，不再发起任何请求；
// - 400 invalid_cursor（以及其他不可重试的 4xx）时停止并报告。

import { authHeaders } from "./auth";
import { ApiError, NetworkError, backoffDelay, pathSeg, readApiError, sleep } from "./client";
import type { FetchLike, TaskEvent } from "./client";

export const TERMINAL_EVENT = "task_terminal";

/** 一个已解析的 SSE 帧（WHATWG HTML §9.2 event stream 解释规则）。 */
export interface SSEFrame {
  id: string | undefined;
  event: string;
  data: string;
}

/**
 * 增量 SSE 解析器。feed() 可接收任意切分的字节块：UTF-8 码点或行被拆在两个块之间都能正确拼接。
 * 支持 LF、CRLF、CR 行尾；注释行（以 ":" 开头）通过 onComment 报告。
 */
export class SSEParser {
  private readonly decoder = new TextDecoder("utf-8");
  private buf = "";
  private pendingCR = false;
  private dataLines: string[] = [];
  private eventType = "";
  private id: string | undefined;
  private sawData = false;

  constructor(
    private readonly onFrame: (f: SSEFrame) => void,
    private readonly onComment: (text: string) => void = () => {},
  ) {}

  feed(chunk: Uint8Array | string): void {
    const text = typeof chunk === "string" ? chunk : this.decoder.decode(chunk, { stream: true });
    this.consume(text);
  }

  /** 流结束：未以空行结束的帧按规范丢弃。 */
  end(): void {
    this.consume(this.decoder.decode());
    this.buf = "";
    this.resetFrame();
  }

  private consume(text: string): void {
    let s = text;
    if (this.pendingCR) {
      // 上一块以 CR 结尾：若本块以 LF 开头，它属于同一个 CRLF。
      if (s.startsWith("\n")) s = s.slice(1);
      this.pendingCR = false;
    }
    this.buf += s;
    let start = 0;
    for (let i = 0; i < this.buf.length; i++) {
      const c = this.buf.charCodeAt(i);
      if (c !== 10 && c !== 13) continue;
      const line = this.buf.slice(start, i);
      if (c === 13) {
        if (i + 1 < this.buf.length) {
          if (this.buf.charCodeAt(i + 1) === 10) i++;
        } else {
          this.pendingCR = true;
        }
      }
      start = i + 1;
      this.line(line);
    }
    this.buf = this.buf.slice(start);
  }

  private line(line: string): void {
    if (line === "") {
      this.dispatch();
      return;
    }
    if (line.startsWith(":")) {
      this.onComment(line.slice(1).replace(/^ /, ""));
      return;
    }
    const colon = line.indexOf(":");
    const field = colon < 0 ? line : line.slice(0, colon);
    let value = colon < 0 ? "" : line.slice(colon + 1);
    if (value.startsWith(" ")) value = value.slice(1);
    switch (field) {
      case "data":
        this.dataLines.push(value);
        this.sawData = true;
        break;
      case "event":
        this.eventType = value;
        break;
      case "id":
        if (!value.includes("\0")) this.id = value;
        break;
      default:
        // retry 与未知字段忽略：重连节奏由客户端自己的退避决定。
        break;
    }
  }

  private dispatch(): void {
    if (this.sawData) {
      this.onFrame({ id: this.id, event: this.eventType || "message", data: this.dataLines.join("\n") });
    }
    this.resetFrame();
  }

  private resetFrame(): void {
    this.dataLines = [];
    this.eventType = "";
    this.id = undefined;
    this.sawData = false;
  }
}

/** 事件流因协议错误（事件缺少有效 id 或 data 不是 JSON）而终止。 */
export class ProtocolError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ProtocolError";
  }
}

export type StreamState = "connecting" | "open" | "reconnecting" | "terminal" | "stopped" | "closed";

export type StreamOutcome =
  | { kind: "terminal"; lastSeq: number }
  | { kind: "closed"; lastSeq: number }
  | { kind: "error"; lastSeq: number; error: ApiError | ProtocolError };

export interface WatchOptions {
  taskId: string;
  baseUrl?: string;
  getToken?: () => string;
  fetch?: FetchLike;
  /** 已交付的最后一个 task_seq；0 表示从头开始。 */
  cursor?: number;
  onEvent: (ev: TaskEvent, frameType: string) => void;
  onHeartbeat?: () => void;
  onState?: (state: StreamState, info: { attempt: number; delayMs?: number; error?: unknown }) => void;
  backoffBaseMs?: number;
  backoffCapMs?: number;
  /** 超过这么久没有收到任何字节（含心跳）则断开重连；默认 45 s = 3 个心跳周期。 */
  idleTimeoutMs?: number;
}

export interface EventStream {
  /** 停止：中止当前连接与等待中的重连。 */
  close(): void;
  /** 当前游标（最后交付的 task_seq）。 */
  readonly cursor: number;
  readonly done: Promise<StreamOutcome>;
}

export function watchTaskEvents(opts: WatchOptions): EventStream {
  const fetchFn: FetchLike = opts.fetch ?? ((input, init) => globalThis.fetch(input, init));
  const getToken = opts.getToken ?? (() => "");
  const baseMs = opts.backoffBaseMs ?? 1000;
  const capMs = opts.backoffCapMs ?? 30_000;
  const idleMs = opts.idleTimeoutMs ?? 45_000;
  const url = `${(opts.baseUrl ?? "").replace(/\/+$/, "")}/tasks/${pathSeg(opts.taskId)}/events`;
  const stopper = new AbortController();
  let cursor = Math.max(0, Math.trunc(opts.cursor ?? 0));
  const state = (s: StreamState, info: { attempt: number; delayMs?: number; error?: unknown }) =>
    opts.onState?.(s, info);

  type Once = { terminal: boolean; progressed: boolean; error?: unknown };

  async function connectOnce(attempt: number): Promise<Once> {
    const conn = new AbortController();
    const abortConn = () => conn.abort(stopper.signal.reason);
    stopper.signal.addEventListener("abort", abortConn, { once: true });
    let idleTimer: ReturnType<typeof setTimeout> | undefined;
    const armIdle = () => {
      clearTimeout(idleTimer);
      idleTimer = setTimeout(() => conn.abort(new NetworkError("事件流空闲超时")), idleMs);
    };
    const out: Once = { terminal: false, progressed: false };
    try {
      state("connecting", { attempt });
      const headers: Record<string, string> = { Accept: "text/event-stream", ...authHeaders(getToken()) };
      if (cursor > 0) headers["Last-Event-ID"] = String(cursor);
      armIdle();
      let resp: Response;
      try {
        resp = await fetchFn(url, { method: "GET", headers, signal: conn.signal, cache: "no-store", credentials: "same-origin" });
      } catch (e) {
        out.error = e instanceof NetworkError ? e : new NetworkError(e instanceof Error ? e.message : String(e), { cause: e });
        return out;
      }
      if (resp.status !== 200) {
        out.error = await readApiError(resp);
        return out;
      }
      if (!resp.body) {
        out.error = new NetworkError("事件流没有响应体");
        return out;
      }
      state("open", { attempt });
      let fatal: ProtocolError | undefined;
      const parser = new SSEParser(
        (f) => {
          if (out.terminal || fatal) return;
          const seq = f.id !== undefined && /^\d+$/.test(f.id) ? Number(f.id) : NaN;
          if (!Number.isSafeInteger(seq) || seq <= 0) {
            fatal = new ProtocolError(`事件缺少有效的 id: ${JSON.stringify(f.id)}`);
            return;
          }
          if (seq <= cursor) return; // 重连后服务端重放的旧事件：按 ID 去重
          let ev: TaskEvent;
          try {
            ev = JSON.parse(f.data) as TaskEvent;
          } catch {
            fatal = new ProtocolError(`事件 ${seq} 的 data 不是 JSON`);
            return;
          }
          cursor = seq;
          out.progressed = true;
          if (f.event === TERMINAL_EVENT || ev.type === TERMINAL_EVENT) out.terminal = true;
          opts.onEvent(ev, f.event);
        },
        () => opts.onHeartbeat?.(),
      );
      const reader = resp.body.getReader();
      try {
        for (;;) {
          const { done, value } = await reader.read();
          if (done) break;
          armIdle();
          parser.feed(value);
          if (out.terminal || fatal) break;
        }
        if (!out.terminal && !fatal) parser.end();
      } catch (e) {
        out.error = e instanceof NetworkError ? e : new NetworkError(e instanceof Error ? e.message : String(e), { cause: e });
      } finally {
        if (out.terminal || fatal || conn.signal.aborted) {
          // 主动关闭连接；忽略取消本身的错误。
          reader.cancel().catch(() => {});
        }
      }
      if (fatal) out.error = fatal;
      return out;
    } finally {
      clearTimeout(idleTimer);
      stopper.signal.removeEventListener("abort", abortConn);
    }
  }

  async function run(): Promise<StreamOutcome> {
    let failures = 0; // 连续无进展的尝试次数
    for (let attempt = 0; ; attempt++) {
      if (stopper.signal.aborted) break;
      const r = await connectOnce(attempt);
      if (r.terminal) {
        state("terminal", { attempt });
        return { kind: "terminal", lastSeq: cursor };
      }
      if (stopper.signal.aborted) break;
      if (r.error instanceof ProtocolError || (r.error instanceof ApiError && !r.error.retryable)) {
        // 包括 400 invalid_cursor：游标非法或超出最新事件，重连不会改善，停止并报告。
        state("stopped", { attempt, error: r.error });
        return { kind: "error", lastSeq: cursor, error: r.error };
      }
      if (r.progressed) failures = 0;
      // 有进展的断线立即续传；无进展时按 1 s、2 s、4 s … 30 s 退避。
      const delayMs = r.progressed ? 0 : backoffDelay(failures++, baseMs, capMs);
      state("reconnecting", { attempt: attempt + 1, delayMs, error: r.error });
      if (delayMs > 0) {
        try {
          await sleep(delayMs, stopper.signal);
        } catch {
          break;
        }
      }
    }
    state("closed", { attempt: 0 });
    return { kind: "closed", lastSeq: cursor };
  }

  const done = run();
  return {
    close: () => stopper.abort(new DOMException("closed", "AbortError")),
    get cursor() {
      return cursor;
    },
    done,
  };
}

// 测试辅助：伪造 fetch 与流式响应（只在 *.test.ts 中使用）。

export interface Call {
  url: string;
  init: RequestInit;
  headers: Record<string, string>;
  body: string | undefined;
}

export function headersOf(init: RequestInit | undefined): Record<string, string> {
  const out: Record<string, string> = {};
  new Headers(init?.headers).forEach((v, k) => {
    out[k.toLowerCase()] = v;
  });
  return out;
}

/** 依次输出给定块的 text/event-stream 响应；hold = true 时输出完不关闭（模拟长连接），直到被中止。 */
export function sseResponse(chunks: (string | Uint8Array)[], opts: { hold?: boolean; signal?: AbortSignal | null } = {}): Response {
  const enc = new TextEncoder();
  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      for (const c of chunks) controller.enqueue(typeof c === "string" ? enc.encode(c) : c);
      if (!opts.hold) {
        controller.close();
        return;
      }
      opts.signal?.addEventListener("abort", () => {
        try {
          controller.error(opts.signal?.reason ?? new Error("aborted"));
        } catch {
          // 已关闭
        }
      });
    },
  });
  return new Response(stream, { status: 200, headers: { "Content-Type": "text/event-stream" } });
}

export function jsonResponse(status: number, body: unknown): Response {
  return new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });
}

export function frame(seq: number, type: string, extra: Record<string, unknown> = {}): string {
  const data = { task_seq: seq, source: "host", type, ts: "2026-10-05T00:00:00Z", ...extra };
  return `id: ${seq}\nevent: ${type}\ndata: ${JSON.stringify(data)}\n\n`;
}

/** 记录每次调用并按脚本返回响应的伪 fetch。 */
export function scriptedFetch(script: (call: Call, n: number) => Promise<Response> | Response) {
  const calls: Call[] = [];
  const fn = async (url: string, init: RequestInit = {}): Promise<Response> => {
    const call: Call = { url, init, headers: headersOf(init), body: typeof init.body === "string" ? init.body : undefined };
    calls.push(call);
    return script(call, calls.length - 1);
  };
  return { fn, calls };
}

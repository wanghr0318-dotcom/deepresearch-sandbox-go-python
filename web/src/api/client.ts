// agentbox HTTP API 客户端（规格 §15.1、§15.4；契约 api/openapi.yaml）。
// - 只用 fetch；token 只放在 Authorization 头，从不进入 URL。
// - 非 2xx 的错误体映射为 ApiError（code/message）。
// - 写请求带 request_id；可重试错误（网络错误、5xx 除 501 与 503 服务模式错误）按有界退避重试，
//   请求体在重试间保持不变，所以 request_id 被复用，服务端按幂等语义返回首次结果。

import { authHeaders } from "./auth";
import type { components } from "./schema";

export type Schemas = components["schemas"];
export type Status = Schemas["Status"];
export type Task = Schemas["Task"];
export type TaskList = Schemas["TaskList"];
export type TaskEvent = Schemas["Event"];
export type CreateTaskRequest = Schemas["CreateTaskRequest"];
export type CreateTaskResult = Schemas["CreateTaskResult"];
export type ControlRequest = Schemas["ControlRequest"];
export type ControlResult = Schemas["ControlResult"];
export type TaskResult = Schemas["Result"];
export type Inspection = Schemas["Inspection"];
export type ErrorBody = Schemas["Error"];

export type FetchLike = (input: string, init?: RequestInit) => Promise<Response>;

/** 服务端返回的类型化错误；status 为 HTTP 状态码，code 为契约中的错误码。 */
export class ApiError extends Error {
  readonly status: number;
  readonly code: string;

  constructor(status: number, code: string, message: string) {
    super(message || code);
    this.name = "ApiError";
    this.status = status;
    this.code = code;
  }

  /**
   * 暂时性错误可以用同一 request_id 重试：5xx（501 除外）与 429。
   * 服务模式错误（503 diagnostic_mode / ownership_lost）不是暂时性的——服务端在操作员处理前一直拒绝，
   * 重试只会推迟提示，所以不重试，立即交给界面显示。
   */
  get retryable(): boolean {
    if (this.status === 503 && SERVICE_MODE_CODES.has(this.code)) return false;
    return (this.status >= 500 && this.status !== 501) || this.status === 429;
  }
}

/** 表示服务整体处于非正常模式的 503 错误码（契约 Status.mode）；不重试。 */
const SERVICE_MODE_CODES: ReadonlySet<string> = new Set(["diagnostic_mode", "ownership_lost"]);

/** 网络层失败（连接被拒、断开等），总是可重试。 */
export class NetworkError extends Error {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = "NetworkError";
  }
}

function isRecord(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null && !Array.isArray(v);
}

function errorFields(v: unknown): ErrorBody | null {
  if (isRecord(v) && typeof v.code === "string") {
    return { code: v.code, message: typeof v.message === "string" ? v.message : "" };
  }
  return null;
}

/**
 * 把错误响应体映射为 ApiError。契约中的 Error 对象为 {"code","message"}；
 * 也接受包了一层的 {"error":{"code","message"}}。无法解析时 code = "unknown"。
 */
export function parseApiError(status: number, text: string): ApiError {
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    parsed = undefined;
  }
  const fields = errorFields(isRecord(parsed) && "error" in parsed ? parsed.error : parsed);
  if (fields) return new ApiError(status, fields.code, fields.message);
  return new ApiError(status, "unknown", text.trim().slice(0, 500) || `HTTP ${status}`);
}

export async function readApiError(resp: Response): Promise<ApiError> {
  let text = "";
  try {
    text = await resp.text();
  } catch {
    // 读不到响应体时只保留状态码。
  }
  return parseApiError(resp.status, text);
}

/** 指数退避：base·2^n，上限 cap（默认 1 s 起，30 s 封顶）。 */
export function backoffDelay(n: number, baseMs = 1000, capMs = 30_000): number {
  const exp = Math.min(Math.max(n, 0), 30);
  return Math.min(baseMs * 2 ** exp, capMs);
}

export function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) {
      reject(signal.reason);
      return;
    }
    const onAbort = () => {
      clearTimeout(timer);
      reject(signal?.reason);
    };
    const timer = setTimeout(() => {
      signal?.removeEventListener("abort", onAbort);
      resolve();
    }, ms);
    signal?.addEventListener("abort", onAbort, { once: true });
  });
}

export function newRequestId(): string {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return crypto.randomUUID();
  }
  const bytes = new Uint8Array(16);
  crypto.getRandomValues(bytes);
  return Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("");
}

export function pathSeg(s: string): string {
  return encodeURIComponent(s);
}

export interface ClientOptions {
  /** API 根地址；默认空串 = 与工作台同源（§15.3）。 */
  baseUrl?: string;
  /** 每次请求时读取当前 token（来自 TokenStore）。 */
  getToken?: () => string;
  fetch?: FetchLike;
  /** 单个请求的最多尝试次数（含首次）。 */
  maxAttempts?: number;
  backoffBaseMs?: number;
  backoffCapMs?: number;
}

export interface Download {
  blob: Blob;
  contentType: string;
  etag: string;
  /** 服务端对主动内容给出 attachment；调用方据此只下载、不内联。 */
  contentDisposition: string;
}

export class ApiClient {
  private readonly baseUrl: string;
  private readonly getToken: () => string;
  private readonly fetchFn: FetchLike;
  private readonly maxAttempts: number;
  private readonly backoffBaseMs: number;
  private readonly backoffCapMs: number;

  constructor(opts: ClientOptions = {}) {
    this.baseUrl = (opts.baseUrl ?? "").replace(/\/+$/, "");
    this.getToken = opts.getToken ?? (() => "");
    this.fetchFn = opts.fetch ?? ((input, init) => globalThis.fetch(input, init));
    this.maxAttempts = Math.max(1, opts.maxAttempts ?? 4);
    this.backoffBaseMs = opts.backoffBaseMs ?? 500;
    this.backoffCapMs = opts.backoffCapMs ?? 30_000;
  }

  /** 发送一次请求（带可重试错误的退避重试），返回 2xx 响应。 */
  async send(method: string, path: string, body?: unknown, accept = "application/json"): Promise<Response> {
    // 请求体只序列化一次：重试时字节完全相同，request_id 随之复用。
    const payload = body === undefined ? undefined : JSON.stringify(body);
    let lastErr: unknown;
    for (let n = 0; n < this.maxAttempts; n++) {
      if (n > 0) await sleep(backoffDelay(n - 1, this.backoffBaseMs, this.backoffCapMs));
      const headers: Record<string, string> = { Accept: accept, ...authHeaders(this.getToken()) };
      if (payload !== undefined) headers["Content-Type"] = "application/json";
      let resp: Response;
      try {
        resp = await this.fetchFn(this.baseUrl + path, {
          method,
          headers,
          body: payload,
          credentials: "same-origin",
          cache: "no-store",
        });
      } catch (e) {
        lastErr = new NetworkError(e instanceof Error ? e.message : String(e), { cause: e });
        continue;
      }
      if (resp.ok) return resp;
      const err = await readApiError(resp);
      if (!err.retryable) throw err;
      lastErr = err;
    }
    throw lastErr;
  }

  private async json<T>(method: string, path: string, body?: unknown): Promise<T> {
    const resp = await this.send(method, path, body);
    return (await resp.json()) as T;
  }

  getStatus(): Promise<Status> {
    return this.json("GET", "/status");
  }

  listTasks(params: { after?: string; limit?: number } = {}): Promise<TaskList> {
    const q = new URLSearchParams();
    if (params.after) q.set("after", params.after);
    if (params.limit !== undefined) q.set("limit", String(params.limit));
    const qs = q.toString();
    return this.json("GET", "/tasks" + (qs ? `?${qs}` : ""));
  }

  getTask(id: string): Promise<Task> {
    return this.json("GET", `/tasks/${pathSeg(id)}`);
  }

  /** 创建任务；requestId 缺省时生成一个，并在本次调用的所有重试中复用。 */
  createTask(input: Omit<CreateTaskRequest, "request_id">, requestId = newRequestId()): Promise<CreateTaskResult> {
    const body: CreateTaskRequest = { request_id: requestId, ...input };
    return this.json("POST", "/tasks", body);
  }

  private control(action: "cancel" | "pause" | "resume", id: string, reason?: string, requestId = newRequestId()) {
    const body: ControlRequest = { request_id: requestId };
    if (reason) body.reason = reason;
    return this.json<ControlResult>("POST", `/tasks/${pathSeg(id)}/${action}`, body);
  }

  cancelTask(id: string, reason?: string, requestId?: string): Promise<ControlResult> {
    return this.control("cancel", id, reason, requestId);
  }

  pauseTask(id: string, reason?: string, requestId?: string): Promise<ControlResult> {
    return this.control("pause", id, reason, requestId);
  }

  resumeTask(id: string, reason?: string, requestId?: string): Promise<ControlResult> {
    return this.control("resume", id, reason, requestId);
  }

  getResult(id: string): Promise<TaskResult> {
    return this.json("GET", `/tasks/${pathSeg(id)}/result`);
  }

  inspectTask(id: string): Promise<Inspection> {
    return this.json("GET", `/tasks/${pathSeg(id)}/inspect`);
  }

  /** 下载一个固定版本的产物（规范路径 /versions/{v}）。token 走请求头，因此不能用 <a href> 直链。 */
  async downloadArtifact(taskId: string, artifactId: string, version: number): Promise<Download> {
    const resp = await this.send(
      "GET",
      `/tasks/${pathSeg(taskId)}/artifacts/${pathSeg(artifactId)}/versions/${pathSeg(String(version))}`,
      undefined,
      "*/*",
    );
    return {
      blob: await resp.blob(),
      contentType: resp.headers.get("Content-Type") ?? "application/octet-stream",
      etag: resp.headers.get("ETag") ?? "",
      contentDisposition: resp.headers.get("Content-Disposition") ?? "",
    };
  }
}

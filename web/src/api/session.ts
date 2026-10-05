// 用户侧（DeepResearch 助手）的 API 客户端：cookie 会话模式。
// - 登录/注册成功后，服务端下发 HttpOnly 的 agentbox_session cookie；脚本从不读取、保存或拼接会话 ID，
//   它只由浏览器随同源请求自动携带（fetch credentials: 'same-origin'）。
// - 不发送 Authorization 头：这里的 ApiClient 没有 token 来源，与运维工作台的 TokenStore 完全隔离。
// - 注册/登录/登出不重试（避免重复注册与放大限速）；研究提交带 request_id，可安全重试。

import { ApiClient, ApiError, newRequestId } from "./client";
import type { ClientOptions, ControlResult, CreateTaskResult, Download, ResearchRequest, Task, TaskList, TaskResult, User } from "./client";
import { watchTaskEvents } from "./sse";
import type { EventStream, WatchOptions } from "./sse";

export type SessionOptions = Pick<ClientOptions, "baseUrl" | "fetch" | "maxAttempts" | "backoffBaseMs" | "backoffCapMs">;

/** 用户名规则（与服务端一致）：3–32 位，字母、数字、下划线、点、连字符。 */
export const USERNAME_RE = /^[A-Za-z0-9_.-]{3,32}$/;
export const PASSWORD_MIN = 8;
export const PASSWORD_MAX = 128;
export const TOPIC_MAX = 500;

/** 按 Unicode 码点计数（与服务端按 rune 计一致）。 */
export function runeLength(s: string): number {
  return [...s].length;
}

/** 校验注册表单；返回面向用户的中文提示，合法时返回空串。 */
export function validateRegistration(username: string, password: string, confirm: string): string {
  if (!USERNAME_RE.test(username.trim())) return "用户名需为 3–32 位字母、数字、下划线、点或连字符";
  const n = runeLength(password);
  if (n < PASSWORD_MIN || n > PASSWORD_MAX) return `密码需为 ${PASSWORD_MIN}–${PASSWORD_MAX} 个字符`;
  if (password !== confirm) return "两次输入的密码不一致";
  return "";
}

export function validateLogin(username: string, password: string): string {
  if (!username.trim() || !password) return "请输入用户名和密码";
  return "";
}

export function validateTopic(topic: string): string {
  const t = topic.trim();
  if (!t) return "请先输入想研究的主题";
  if (runeLength(t) > TOPIC_MAX) return `主题不能超过 ${TOPIC_MAX} 个字符`;
  return "";
}

export class SessionApi {
  /** 注册、登录、登出、me：只尝试一次。 */
  private readonly once: ApiClient;
  /** 读取与幂等写（带 request_id）：沿用 ApiClient 的有界重试。 */
  private readonly client: ApiClient;
  private readonly opts: SessionOptions;

  constructor(opts: SessionOptions = {}) {
    this.opts = opts;
    // 刻意不传 getToken：用户请求永远不带 Authorization 头。
    this.once = new ApiClient({ ...opts, maxAttempts: 1 });
    this.client = new ApiClient(opts);
  }

  private async json<T>(c: ApiClient, method: string, path: string, body?: unknown): Promise<T> {
    const resp = await c.send(method, path, body);
    return (await resp.json()) as T;
  }

  register(username: string, password: string): Promise<User> {
    return this.json(this.once, "POST", "/auth/register", { username: username.trim(), password });
  }

  login(username: string, password: string): Promise<User> {
    return this.json(this.once, "POST", "/auth/login", { username: username.trim(), password });
  }

  /** 登出；会话已失效（401）也视为已登出。 */
  async logout(): Promise<void> {
    try {
      await this.once.send("POST", "/auth/logout");
    } catch (e) {
      if (!(e instanceof ApiError && e.status === 401)) throw e;
    }
  }

  /** 当前登录用户；未登录（401）返回 null。 */
  async me(): Promise<User | null> {
    try {
      return await this.json<User>(this.once, "GET", "/auth/me");
    } catch (e) {
      if (e instanceof ApiError && e.status === 401) return null;
      throw e;
    }
  }

  /** 提交研究主题；requestId 缺省时生成一个，并在本次调用的重试中复用。 */
  createResearch(topic: string, requestId = newRequestId()): Promise<CreateTaskResult> {
    const body: ResearchRequest = { request_id: requestId, topic: topic.trim() };
    return this.json(this.client, "POST", "/research", body);
  }

  listTasks(params: { after?: string; limit?: number } = {}): Promise<TaskList> {
    return this.client.listTasks(params);
  }

  getTask(id: string): Promise<Task> {
    return this.client.getTask(id);
  }

  getResult(id: string): Promise<TaskResult> {
    return this.client.getResult(id);
  }

  cancelTask(id: string, requestId?: string): Promise<ControlResult> {
    return this.client.cancelTask(id, undefined, requestId);
  }

  downloadArtifact(taskId: string, artifactId: string, version: number): Promise<Download> {
    return this.client.downloadArtifact(taskId, artifactId, version);
  }

  /** 订阅任务事件（服务端对用户按允许列表脱敏）；同样只靠 cookie 鉴权。 */
  watch(opts: Pick<WatchOptions, "taskId" | "cursor" | "onEvent" | "onState">): EventStream {
    return watchTaskEvents({ ...opts, baseUrl: this.opts.baseUrl, fetch: this.opts.fetch });
  }
}

export type SessionApiLike = Pick<
  SessionApi,
  | "register"
  | "login"
  | "logout"
  | "me"
  | "createResearch"
  | "listTasks"
  | "getTask"
  | "getResult"
  | "cancelTask"
  | "downloadArtifact"
>;

/** 用户页共享的单例（同源）。 */
export const sessionApi = new SessionApi();

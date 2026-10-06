// 对话会话 API 客户端（设计 §6；契约 api/openapi.yaml 的 /sessions、/turns）。
// - 与 SessionApi 相同的 cookie 会话模式：不发 Authorization 头，浏览器随同源请求携带 HttpOnly cookie。
// - 写请求带 request_id；可重试错误按 ApiClient 的有界退避重试，请求体不变，request_id 随之复用。
// - 会话事件流 /sessions/<id>/events 跨越多轮一直打开，按 Last-Event-ID 续传；只有 session_state closed 是终点。

import { ApiClient, newRequestId, pathSeg } from "./client";
import type { Download, Schemas } from "./client";
import { runeLength } from "./session";
import type { SessionOptions } from "./session";
import { watchEvents } from "./sse";
import type { EventStream, StreamOptions } from "./sse";

// 契约类型的别名集中在这里：OpenAPI 改名时只改这几行。
export type ChatSession = Schemas["Session"];
export type SessionList = Schemas["SessionList"];
export type Turn = Schemas["Turn"];
export type TurnList = Schemas["TurnList"];
export type SessionEvent = Schemas["SessionEvent"];
export type SessionEventType = Schemas["SessionEventType"];
export type MessageRequest = Schemas["MessageRequest"];
export type MessageResult = Schemas["MessageResult"];
export type TurnControlResult = Schemas["TurnControlResult"];
export type AnswerRequest = Schemas["AnswerRequest"];
/** 一题的回答：{question_id（ask_user 中该题的 id）, choice? | other?}，二者恰有其一。 */
export type Answer = Schemas["Answer"];
export type RawRefData = Schemas["RawRef"];
export type CreateSessionRequest = Schemas["CreateSessionRequest"];
export type RenameSessionRequest = Schemas["RenameSessionRequest"];
export type SessionWriteRequest = Schemas["SessionWriteRequest"];

/** 停止、继续、立即写报告（恢复会新开一轮，见 ChatApi.restore）。 */
export type TurnAction = "stop" | "continue" | "finish";

/** GET /turns/{id}/raw/{sha} 的内容：JSON 解析后的值，或非 JSON 内容的原文。 */
export interface RawContent {
  contentType: string;
  body: unknown;
}

export const MESSAGE_MAX = 4000;
export const TITLE_MAX = 80;
const TURN_PAGE = 100;

/** 校验输入框内容（与服务端一致：去掉首尾空白后 1–4000 个字符，按码点计）。 */
export function validateMessage(text: string): string {
  const t = text.trim();
  if (!t) return "请输入内容";
  if (runeLength(t) > MESSAGE_MAX) return `消息不能超过 ${MESSAGE_MAX} 个字符`;
  return "";
}

/** 校验会话标题（去掉首尾空白后 1–80 个字符）。 */
export function validateTitle(title: string): string {
  const t = title.trim();
  if (!t) return "标题不能为空";
  if (runeLength(t) > TITLE_MAX) return `标题不能超过 ${TITLE_MAX} 个字符`;
  return "";
}

export interface WatchSessionOptions {
  sessionId: string;
  cursor?: number;
  onEvent: (ev: SessionEvent) => void;
  onState?: StreamOptions<SessionEvent>["onState"];
}

/** 会话流的终点：会话已删除（session_state closed），之后不再重连。 */
export function isSessionClosed(_frameType: string, ev: SessionEvent): boolean {
  return ev?.type === "session_state" && ev.data?.state === "closed";
}

export class ChatApi {
  private readonly client: ApiClient;
  private readonly opts: SessionOptions;

  constructor(opts: SessionOptions = {}) {
    this.opts = opts;
    // 刻意不传 getToken：用户请求永远不带 Authorization 头。
    this.client = new ApiClient(opts);
  }

  private async json<T>(method: string, path: string, body?: unknown): Promise<T> {
    const resp = await this.client.send(method, path, body);
    return (await resp.json()) as T;
  }

  listSessions(params: { after?: string; limit?: number } = {}): Promise<SessionList> {
    const q = new URLSearchParams();
    if (params.after) q.set("after", params.after);
    if (params.limit !== undefined) q.set("limit", String(params.limit));
    const qs = q.toString();
    return this.json("GET", "/sessions" + (qs ? `?${qs}` : ""));
  }

  getSession(id: string): Promise<ChatSession> {
    return this.json("GET", `/sessions/${pathSeg(id)}`);
  }

  createSession(requestId = newRequestId()): Promise<ChatSession> {
    const body: CreateSessionRequest = { request_id: requestId };
    return this.json("POST", "/sessions", body);
  }

  /** 重命名（标题去掉首尾空白后 1–80 字）。 */
  renameSession(id: string, title: string): Promise<ChatSession> {
    const body: RenameSessionRequest = { title: title.trim() };
    return this.json("PATCH", `/sessions/${pathSeg(id)}`, body);
  }

  /** 删除（202，会话进入 closing；重复调用幂等）。 */
  deleteSession(id: string): Promise<ChatSession> {
    return this.json("DELETE", `/sessions/${pathSeg(id)}`);
  }

  /** 提前唤醒冻结或被回收的会话（202 无正文；进度见 session_state 事件）。 */
  async wakeSession(id: string, requestId = newRequestId()): Promise<void> {
    const body: SessionWriteRequest = { request_id: requestId };
    await this.client.send("POST", `/sessions/${pathSeg(id)}/wake`, body);
  }

  sendMessage(sessionId: string, text: string, deepResearch: boolean, requestId = newRequestId()): Promise<MessageResult> {
    const body: MessageRequest = { request_id: requestId, text: text.trim(), deep_research: deepResearch };
    return this.json("POST", `/sessions/${pathSeg(sessionId)}/messages`, body);
  }

  /** 会话的全部轮次（turn_index 升序）；按 after_index 翻页直到取完。 */
  async listTurns(sessionId: string): Promise<TurnList> {
    const turns: Turn[] = [];
    let after: number | undefined;
    for (;;) {
      const q = new URLSearchParams({ limit: String(TURN_PAGE) });
      if (after !== undefined) q.set("after_index", String(after));
      const page = await this.json<TurnList>("GET", `/sessions/${pathSeg(sessionId)}/turns?${q.toString()}`);
      turns.push(...page.turns);
      const last = page.turns[page.turns.length - 1];
      if (page.turns.length < TURN_PAGE || !last) break;
      after = last.turn_index;
    }
    return { turns };
  }

  control(turnId: string, action: TurnAction, requestId = newRequestId()): Promise<TurnControlResult> {
    const body: SessionWriteRequest = { request_id: requestId };
    return this.json("POST", `/turns/${pathSeg(turnId)}/${action}`, body);
  }

  /** 恢复被取消的研究：在同一会话新开一轮，从其最后 checkpoint 继续。 */
  restore(turnId: string, requestId = newRequestId()): Promise<MessageResult> {
    const body: SessionWriteRequest = { request_id: requestId };
    return this.json("POST", `/turns/${pathSeg(turnId)}/restore`, body);
  }

  /** 回答 ask_user（每题一条，question_id 为该题的 id）。 */
  answer(turnId: string, answers: Answer[], requestId = newRequestId()): Promise<TurnControlResult> {
    const body: AnswerRequest = { request_id: requestId, answers };
    return this.json("POST", `/turns/${pathSeg(turnId)}/answer`, body);
  }

  /** 原始响应（⟨/⟩）：步骤 raw.response_ref 指向的内容，服务端已对用户脱敏。 */
  async getRaw(turnId: string, sha256: string): Promise<RawContent> {
    const resp = await this.client.send("GET", `/turns/${pathSeg(turnId)}/raw/${pathSeg(sha256)}`, undefined, "application/json, */*");
    const contentType = resp.headers.get("Content-Type") ?? "application/octet-stream";
    const text = await resp.text();
    if (/^application\/(?:[\w.+-]*\+)?json\b/i.test(contentType)) {
      try {
        return { contentType: "application/json", body: JSON.parse(text) as unknown };
      } catch {
        // 声明为 JSON 却解析失败：按原文显示。
      }
    }
    return { contentType, body: text };
  }

  /** 下载报告（turn 即 task，所有者可读；固定版本路径）。 */
  downloadArtifact(turnId: string, artifactId: string, version: number): Promise<Download> {
    return this.client.downloadArtifact(turnId, artifactId, version);
  }

  /** 订阅整个会话的事件流（跨轮次一直打开；按 Last-Event-ID 续传）。 */
  watchSession(opts: WatchSessionOptions): EventStream {
    return watchEvents<SessionEvent>({
      path: `/sessions/${pathSeg(opts.sessionId)}/events`,
      baseUrl: this.opts.baseUrl,
      fetch: this.opts.fetch,
      backoffBaseMs: this.opts.backoffBaseMs,
      backoffCapMs: this.opts.backoffCapMs,
      cursor: opts.cursor,
      onEvent: (ev) => opts.onEvent(ev),
      onState: opts.onState,
      isTerminal: isSessionClosed,
    });
  }
}

export type ChatApiLike = Omit<ChatApi, "watchSession">;

/** 用户页共享的单例（同源）。 */
export const chatApi = new ChatApi();

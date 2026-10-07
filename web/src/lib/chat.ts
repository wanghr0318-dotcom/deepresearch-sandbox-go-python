// 对话事件归约器（设计 §6–§7）：把会话事件流归约为每轮的视图（步骤行、回复、提问、停止卡、待办、来源、额度、报告）。
// - 纯函数，不依赖 Vue；不修改传入的状态（未变化的部分按引用共享）。
// - 按 seq 幂等：seq ≤ cursor 的事件（重连后的重放）原样返回同一个状态对象；未知 type 只推进游标。
// - 事件按 turn_id 归到所属轮次，所以被取代轮次的取消晚于下一轮的事件到达也能正确落位。
// - 原始请求/响应（⟨/⟩）只给经 Gateway 的调用（有 response_ref）；本地工具的步骤行只显示事件 data。
// - 失败只显示面向用户的文案，从不显示原始错误码。

import { ApiError } from "../api/client";
import type { SessionEvent, Turn } from "../api/chat";
import { userErrorMessage } from "./research";

export type StepKind = "skill" | "thinking" | "ask" | "todo" | "search" | "fetch" | "source" | "subtopic" | "code" | "tool";

export interface SearchItem {
  title: string;
  site: string;
  snippet: string;
  url: string;
}

/** ⟨/⟩：内联的请求（已脱敏，可能被截断为字符串）与响应 blob 的 sha（经 GET /turns/{id}/raw/{sha} 读取）。 */
export interface RawRef {
  request: unknown;
  requestTruncated: boolean;
  responseRef: string;
}

export interface StepRow {
  /** tool_call_id，或 "<type>-<seq>" */
  id: string;
  kind: StepKind;
  /** 例："搜索网页（5 条结果）"、"阅读网页 · a.example"、"读取 skill：deep-research"、"思考" */
  title: string;
  status: "running" | "done" | "error";
  /** 补充说明：搜索词、网页地址 */
  detail?: string;
  /** 所属子主题的 id 与标题 */
  subtopicId?: string;
  subtopic?: string;
  /** search 展开后的逐行结果 */
  results?: SearchItem[];
  /** thinking 文本、fetch 摘录、本地工具结果、子主题摘要、失败说明 */
  text?: string;
  raw?: RawRef;
}

export interface TodoItem {
  id: string;
  title: string;
  status: "pending" | "in_progress" | "done" | "skipped";
  /** 计划给这一项的工具调用次数 */
  budgetShare?: number;
}

export interface SubtopicItem {
  id: string;
  title: string;
  status: "running" | "done" | "skipped" | "failed";
  summary?: string;
}

export interface SourceItem {
  n: number;
  title: string;
  url: string;
  site: string;
}

export interface QuestionItem {
  /** 回答时作为 Answer.question_id */
  id: string;
  question: string;
  options: string[];
  allowOther: boolean;
}

export interface Question {
  questionId: string;
  questions: QuestionItem[];
  /** 轮次已带着回答继续（由事件推出，重放后同样成立） */
  answered: boolean;
  /** 本页提交的回答文本（recordAnswers；刷新后没有） */
  answers?: string[];
}

export interface StopInfo {
  todo: TodoItem[];
  subtopicsDone: number;
  subtopicsTotal: number;
  sources: number;
  budget: { used: number; limit: number };
  findings: string;
  canFinish: boolean;
}

export interface ReportRef {
  artifactId: string;
  version: number;
  title: string;
  partial: boolean;
  toolBudgetReached: boolean;
  note?: string;
}

export interface TurnView {
  turnId: string;
  index: number;
  userText: string;
  deepResearch: boolean;
  route?: "answer" | "research";
  routeForced?: boolean;
  /** queued | running | stopping | paused | awaiting_input | succeeded | failed | cancelled */
  status: string;
  statusReason?: string;
  steps: StepRow[];
  /** assistant_delta 累积（不同 message_id 之间空一行） */
  reply: string;
  /** 最后一个 assistant_delta 的 message_id */
  messageId?: string;
  question?: Question;
  stop?: StopInfo;
  todo: TodoItem[];
  subtopics: SubtopicItem[];
  sources: SourceItem[];
  budget?: { used: number; limit: number };
  report?: ReportRef;
  /** 面向用户的失败提示 */
  error?: string;
  restoredFrom?: string;
  /** 已取消、可从最后 checkpoint 恢复，且还没有被恢复过 */
  canRestore: boolean;
  /** 本轮第一个与最后一个事件的时间（毫秒，取自事件 ts），用于"研究过程 · 用时"与"正在撰写报告 · N 秒" */
  startedAt?: number;
  lastEventAt?: number;
}

export interface ChatState {
  sessionStatus: string;
  /** 会话级提示（例如恢复失败时的"会话暂时无法恢复"） */
  sessionMessage?: string;
  turns: TurnView[];
  /** 已应用的最后一个 seq（续传用的 Last-Event-ID） */
  cursor: number;
}

/** 面向用户的错误文案（按错误码或失败原因）。 */
export const USER_ERRORS: Record<string, string> = {
  model_unavailable: "模型服务暂时不可用，请重试",
  session_unavailable: "会话暂时无法恢复",
  sessions_unavailable: "对话服务暂未开放，请稍后再试",
  session_not_found: "找不到这个对话，可能已被删除",
  session_closed: "这个对话正在删除",
  turn_not_found: "找不到这一轮对话",
  turn_in_progress: "当前研究还在进行，请先停止当前研究",
  user_task_running: "你有另一项研究正在进行，请先停止它",
  invalid_turn_state: "这一轮的状态已经变化，请刷新后再试",
  not_restorable: "这项研究无法恢复",
  invalid_text: "消息需为 1–4000 个字符",
  invalid_title: "标题需为 1–80 个字符",
  invalid_answers: "请回答每一个问题",
  request_conflict: "请求冲突，请刷新后再试",
};

const FAILED_DEFAULT = "本轮未能完成，请重试";

/** API 错误 → 面向用户的文案。 */
export function chatErrorMessage(e: unknown): string {
  if (e instanceof ApiError) {
    const msg = USER_ERRORS[e.code];
    if (msg) return msg;
  }
  return userErrorMessage(e);
}

/** 网址的站点名（去掉 www.）；无法解析时为空串。 */
export function siteOf(url: string): string {
  try {
    return new URL(url).hostname.toLowerCase().replace(/^www\./, "");
  } catch {
    return "";
  }
}

export function emptyChat(): ChatState {
  return { sessionStatus: "idle", turns: [], cursor: 0 };
}

function blankTurn(turnId: string): TurnView {
  return {
    turnId,
    index: Number.MAX_SAFE_INTEGER,
    userText: "",
    deepResearch: false,
    status: "queued",
    steps: [],
    reply: "",
    todo: [],
    subtopics: [],
    sources: [],
    canRestore: false,
  };
}

function sortTurns(turns: TurnView[]): TurnView[] {
  return turns.slice().sort((a, b) => a.index - b.index);
}

function restoredSet(turns: TurnView[]): Set<string> {
  const out = new Set<string>();
  for (const t of turns) if (t.restoredFrom) out.add(t.restoredFrom);
  return out;
}

/** 由历史（GET /sessions/{id}/turns）建立或刷新轮次；已有轮次的实时内容（步骤、回复等）保留。 */
export function seedTurns(state: ChatState, turns: Turn[]): ChatState {
  const byId = new Map(state.turns.map((t) => [t.turnId, t] as const));
  for (const h of turns) {
    const prev = byId.get(h.turn_id) ?? blankTurn(h.turn_id);
    const t: TurnView = {
      ...prev,
      index: h.turn_index,
      userText: h.text,
      deepResearch: h.deep_research,
      status: h.status,
      statusReason: h.status_reason,
      route: h.route ?? prev.route,
      restoredFrom: h.restored_from_turn_id ?? prev.restoredFrom,
      canRestore: h.restorable,
    };
    if (h.tool_call_limit > 0) t.budget = { used: h.tool_calls_used, limit: h.tool_call_limit };
    if (h.report && !prev.report) {
      t.report = { artifactId: h.report.artifact_id, version: h.report.version, title: "研究报告", partial: false, toolBudgetReached: false };
    }
    if (!t.reply && h.summary) t.reply = h.summary;
    t.error = h.status === "failed" ? h.user_message || FAILED_DEFAULT : undefined;
    byId.set(h.turn_id, t);
  }
  let list = [...byId.values()];
  const restored = restoredSet(list);
  list = list.map((t) => (t.canRestore && restored.has(t.turnId) ? { ...t, canRestore: false } : t));
  return { ...state, turns: sortTurns(list) };
}

// ---- 事件归约 ----

type Data = Record<string, unknown>;

function isRecord(v: unknown): v is Data {
  return typeof v === "object" && v !== null && !Array.isArray(v);
}
function str(v: unknown): string {
  return typeof v === "string" ? v : "";
}
function optStr(v: unknown): string | undefined {
  return typeof v === "string" && v !== "" ? v : undefined;
}
function num(v: unknown): number {
  return typeof v === "number" && Number.isFinite(v) ? v : 0;
}
function arr(v: unknown): unknown[] {
  return Array.isArray(v) ? v : [];
}

const TODO_STATUS = new Set(["pending", "in_progress", "done", "skipped"]);
const SUBTOPIC_STATUS = new Set(["running", "done", "skipped", "failed"]);
const SETTLED = new Set(["paused", "awaiting_input", "succeeded", "failed", "cancelled"]);

function toRaw(v: unknown): RawRef | undefined {
  if (!isRecord(v)) return undefined;
  const ref = optStr(v.response_ref);
  if (!ref) return undefined; // 本地工具没有 ⟨/⟩
  return { request: v.request, requestTruncated: v.request_truncated === true, responseRef: ref };
}

function toTodo(v: unknown): TodoItem[] {
  return arr(v)
    .filter(isRecord)
    .map((x) => {
      const item: TodoItem = {
        id: str(x.id),
        title: str(x.title),
        status: (TODO_STATUS.has(str(x.status)) ? x.status : "pending") as TodoItem["status"],
      };
      if (typeof x.budget_share === "number") item.budgetShare = x.budget_share;
      return item;
    });
}

function kindOfTool(tool: string): StepKind {
  switch (tool) {
    case "web_search":
      return "search";
    case "web_fetch":
      return "fetch";
    case "read_source":
      return "source";
    case "read_skill":
      return "skill";
    case "ask_user":
      return "ask";
    case "todo_write":
      return "todo";
    case "research_subtopic":
      return "subtopic";
    case "run_python":
      return "code";
    default:
      return "tool";
  }
}

function subtopicTitle(t: TurnView, id: string | undefined): string | undefined {
  if (!id) return undefined;
  return t.subtopics.find((s) => s.id === id)?.title ?? t.todo.find((x) => x.id === id)?.title;
}

function callTitle(t: TurnView, tool: string, input: Data): { title: string; detail?: string } {
  switch (tool) {
    case "web_search":
      return { title: "搜索网页", detail: optStr(input.query) };
    case "web_fetch": {
      const url = str(input.url);
      const site = siteOf(url);
      return { title: site ? `阅读网页 · ${site}` : "阅读网页", detail: optStr(url) };
    }
    case "read_source": {
      const n = typeof input.n === "number" ? ` [${input.n}]` : "";
      return { title: `查阅来源${n}` };
    }
    case "read_skill": {
      const name = str(input.name);
      return { title: name ? `读取 skill：${name}` : "读取 skill" };
    }
    case "ask_user":
      return { title: "向你提问" };
    case "todo_write":
      return { title: "更新待办清单" };
    case "research_subtopic": {
      const id = str(input.id);
      return { title: `研究子主题：${subtopicTitle(t, id) ?? id}` };
    }
    case "run_python":
      // 完整代码在 ⟨/⟩ 的请求中；行内只给第一行非空代码作提示
      return { title: "运行代码", detail: firstCodeLine(input.code) };
    default:
      return { title: `调用工具：${tool}` };
  }
}

const CODE_LINE_MAX = 80;

function firstCodeLine(code: unknown): string | undefined {
  const line = str(code)
    .split("\n")
    .map((x) => x.trim())
    .find((x) => x !== "");
  if (!line) return undefined;
  return line.length > CODE_LINE_MAX ? `${line.slice(0, CODE_LINE_MAX - 1)}…` : line;
}

// 搜索/抓取失败的说明。旧版 Worker 写的是"抓取失败：<Gateway 错误码>（<HTTP 状态文字>）"，例如超时后重试用完的
// "tries_exhausted（Too Many Requests）"——状态文字并不说明原因，会误导用户；这里按错误码改写为中文说明
// （与 Worker 的 agentbox_worker.tools.base.USER_ERRORS 一致）。新版 Worker 已直接写中文，原样显示。
const UNREACHABLE = "{target}无法访问或超时";
const TOOL_ERRORS: Record<string, string> = {
  tries_exhausted: UNREACHABLE,
  upstream_unreachable: UNREACHABLE,
  upstream_unconfirmed: UNREACHABLE,
  call_deadline_exceeded: UNREACHABLE,
  client_timeout: UNREACHABLE,
  upstream_rate_limited: "请求过多，请稍后再试",
  upstream_unavailable: "网站暂时不可用",
  upstream_rejected: "网站拒绝了请求",
  upstream_bad_response: "网站返回了无法识别的响应",
  response_too_large: "内容过大，无法读取",
  too_many_redirects: "重定向次数过多",
  egress_blocked: "不允许访问该网址（安全限制）",
  invalid_url: "网址无效",
  invalid_request: "请求无效",
  tool_budget_exhausted: "已达工具额度",
  budget_exhausted: "已达费用额度",
  budget_insufficient_for_request: "已达费用额度",
  subrun_budget_exhausted: "已达费用额度",
  call_in_progress: "同一请求仍在进行中",
};
const RAW_TOOL_ERROR = /^(搜索|抓取)失败：([a-z][a-z0-9_]*)(?:（[^）]*）)?$/;

function toolErrorText(text: string): string {
  const m = RAW_TOOL_ERROR.exec(text.trim());
  if (!m) return text;
  const [, action, code] = m as unknown as [string, string, string];
  const known = TOOL_ERRORS[code];
  const reason = known ? known.replace("{target}", action === "搜索" ? "搜索服务" : "网页") : `暂时无法完成（${code}）`;
  return `${action}失败：${reason}`;
}

function previewText(p: Data): string | undefined {
  return optStr(p.text) ?? optStr(p.excerpt);
}

function upsertSource(sources: SourceItem[], src: SourceItem): SourceItem[] {
  const i = sources.findIndex((s) => s.n === src.n);
  const out = sources.slice();
  if (i >= 0) out[i] = src;
  else out.push(src);
  return out.sort((a, b) => a.n - b.n);
}

function applyToolCall(t: TurnView, d: Data, seq: number): TurnView {
  const tool = str(d.tool);
  const input = isRecord(d.input) ? d.input : {};
  const id = optStr(d.tool_call_id) ?? `tool_call-${seq}`;
  const subtopicId = optStr(d.subtopic_id) ?? (tool === "research_subtopic" ? optStr(input.id) : undefined);
  const { title, detail } = callTitle(t, tool, input);
  const row: StepRow = { id, kind: kindOfTool(tool), title, status: "running" };
  if (detail) row.detail = detail;
  if (subtopicId) {
    row.subtopicId = subtopicId;
    const st = subtopicTitle(t, subtopicId);
    if (st && tool !== "research_subtopic") row.subtopic = st;
  }
  const raw = toRaw(d.raw);
  if (raw) row.raw = raw;
  const steps = t.steps.slice();
  const i = steps.findIndex((s) => s.id === id);
  if (i >= 0) steps[i] = { ...steps[i]!, ...row }; // 继续后重发同一调用
  else steps.push(row);
  return { ...t, steps };
}

function applyToolResult(t: TurnView, d: Data, seq: number): TurnView {
  const tool = str(d.tool);
  const kind = kindOfTool(tool);
  const callId = optStr(d.tool_call_id);
  const steps = t.steps.slice();
  let i = callId ? steps.findIndex((s) => s.id === callId) : -1;
  if (i < 0 && !callId) {
    // 没有 tool_call_id：配到同一工具最后一个进行中的行。
    for (let k = steps.length - 1; k >= 0; k--) {
      if (steps[k]!.status === "running" && steps[k]!.kind === kind) {
        i = k;
        break;
      }
    }
  }
  let row: StepRow;
  if (i >= 0) {
    row = { ...steps[i]! };
  } else {
    const subtopicId = optStr(d.subtopic_id);
    row = { id: callId ?? `tool_result-${seq}`, kind, ...callTitle(t, tool, {}), status: "running" };
    if (!row.detail) delete row.detail;
    if (subtopicId) {
      row.subtopicId = subtopicId;
      const st = subtopicTitle(t, subtopicId);
      if (st) row.subtopic = st;
    }
  }
  const ok = d.ok === true;
  const p = isRecord(d.preview) ? d.preview : {};
  let sources = t.sources;
  row.status = ok ? "done" : "error";
  if (kind === "search" || p.kind === "search") {
    const results: SearchItem[] = arr(p.results)
      .filter(isRecord)
      .map((x) => {
        const url = str(x.url);
        return { title: str(x.title), url, site: optStr(x.site) ?? siteOf(url), snippet: str(x.snippet) };
      });
    if (ok) {
      row.results = results;
      row.title = `搜索网页（${results.length} 条结果）`;
    }
    const q = optStr(p.query);
    if (q) row.detail = q;
  } else if (kind === "fetch" || p.kind === "fetch") {
    const url = str(p.url) || row.detail || "";
    const site = optStr(p.site) ?? siteOf(url);
    if (site) row.title = `阅读网页 · ${site}`;
    if (url) row.detail = url;
    const text = previewText(p);
    if (text) row.text = text;
    if (ok && typeof p.n === "number") {
      sources = upsertSource(sources, { n: p.n, title: str(p.title) || site || url, url, site });
    }
  } else {
    const text = previewText(p);
    if (text) row.text = text;
  }
  // run_python 失败（非零退出等）时预览含退出码与 stderr 末尾，比 error 的首段更完整
  const codeText = kind === "code" ? previewText(p) : undefined;
  if (!ok) row.text = toolErrorText(codeText ?? optStr(d.error) ?? row.text ?? "调用失败");
  if (!ok && (kind === "fetch" || p.kind === "fetch") && (row.text ?? "").startsWith("页面被拦截")) {
    const site = siteOf(row.detail ?? "");
    row.title = site ? `阅读网页 · ${site} · 页面被拦截` : "阅读网页 · 页面被拦截";
  }
  const raw = toRaw(d.raw);
  if (raw) row.raw = raw;
  if (i >= 0) steps[i] = row;
  else steps.push(row);
  return { ...t, steps, sources };
}

function applySkillRead(t: TurnView, d: Data, seq: number): TurnView {
  const name = str(d.name);
  const file = optStr(d.file);
  const title = `读取 skill：${name}${file ? ` · ${file}` : ""}`;
  const steps = t.steps.slice();
  const last = steps[steps.length - 1];
  // read_skill 工具行（tool_call → tool_result → skill_read）合并为一行。
  if (last && last.kind === "skill" && !last.id.startsWith("skill_read-")) {
    steps[steps.length - 1] = { ...last, title, status: last.status === "error" ? "error" : "done" };
  } else {
    const row: StepRow = { id: `skill_read-${seq}`, kind: "skill", title, status: "done" };
    const desc = optStr(d.description);
    if (desc) row.text = desc;
    steps.push(row);
  }
  return { ...t, steps };
}

function applyAskUser(t: TurnView, d: Data, seq: number): TurnView {
  const question: Question = {
    questionId: str(d.question_id),
    questions: arr(d.questions)
      .filter(isRecord)
      .map((q) => ({
        id: str(q.id),
        question: str(q.question),
        options: arr(q.options).filter((o): o is string => typeof o === "string"),
        allowOther: q.allow_other === true,
      })),
    answered: false,
  };
  const steps = t.steps.slice();
  const last = steps[steps.length - 1];
  if (last && last.kind === "ask" && last.status === "running") {
    steps[steps.length - 1] = { ...last, status: "done" };
  } else {
    steps.push({ id: `ask_user-${seq}`, kind: "ask", title: "向你提问", status: "done" });
  }
  return { ...t, steps, question };
}

function applySubtopic(t: TurnView, d: Data): TurnView {
  const id = str(d.id);
  const status = (SUBTOPIC_STATUS.has(str(d.status)) ? d.status : "running") as SubtopicItem["status"];
  const item: SubtopicItem = { id, title: str(d.title) || subtopicTitle(t, id) || id, status };
  const summary = optStr(d.summary);
  if (summary) item.summary = summary;
  const subtopics = t.subtopics.slice();
  const i = subtopics.findIndex((s) => s.id === id);
  if (i >= 0) subtopics[i] = item;
  else subtopics.push(item);
  const rowStatus: StepRow["status"] = status === "running" ? "running" : status === "failed" ? "error" : "done";
  const steps = t.steps.map((s) => {
    if (s.kind === "subtopic" && s.subtopicId === id) {
      const row: StepRow = { ...s, title: `研究子主题：${item.title}`, status: rowStatus };
      if (summary) row.text = summary;
      return row;
    }
    if (s.subtopicId === id && !s.subtopic) return { ...s, subtopic: item.title };
    return s;
  });
  return { ...t, subtopics, steps };
}

function applyTurnStatus(t: TurnView, d: Data, restored: Set<string>): TurnView {
  const status = str(d.status) || t.status;
  const reason = optStr(d.reason);
  const next: TurnView = { ...t, status, statusReason: reason };
  if (status === "failed") {
    next.error = (reason && USER_ERRORS[reason]) || optStr(d.user_message) || FAILED_DEFAULT;
  } else {
    next.error = undefined;
  }
  if ((status === "queued" || status === "running" || status === "succeeded") && t.question && !t.question.answered && t.status === "awaiting_input") {
    next.question = { ...t.question, answered: true };
  }
  if (status === "queued" || status === "running") next.stop = undefined; // 继续或立即写报告
  if (SETTLED.has(status) && t.steps.some((s) => s.status === "running")) {
    const settled: StepRow["status"] = status === "failed" ? "error" : "done";
    next.steps = t.steps.map((s) => (s.status === "running" ? { ...s, status: settled } : s));
  }
  next.canRestore = status === "cancelled" && (t.stop !== undefined || reason === "superseded") && !restored.has(t.turnId);
  return next;
}

function applyTurnEvent(t: TurnView, ev: SessionEvent, restored: Set<string>): TurnView {
  const d: Data = isRecord(ev.data) ? (ev.data as Data) : {};
  switch (ev.type) {
    case "turn_created": {
      const next: TurnView = { ...t, index: num(d.turn_index) || t.index, userText: str(d.text), deepResearch: d.deep_research === true };
      const from = optStr(d.restored_from_turn_id);
      if (from) next.restoredFrom = from;
      return next;
    }
    case "turn_status":
      return applyTurnStatus(t, d, restored);
    case "route": {
      const route = d.route === "research" ? "research" : d.route === "answer" ? "answer" : undefined;
      return route ? { ...t, route, routeForced: d.forced === true } : t;
    }
    case "skill_read":
      return applySkillRead(t, d, ev.seq);
    case "thinking": {
      // 停止后"继续"会重放同一次模型调用并重发 thinking：宿主对用户去掉了 raw.call_id，
      // 故以步骤与结果 blob（同一调用重放时不变）为键，相同则替换而不是新增一行。
      const raw = toRaw(d.raw);
      const id = raw ? `thinking-${str(d.step_id)}-${raw.responseRef}` : `thinking-${ev.seq}`;
      const row: StepRow = { id, kind: "thinking", title: "思考", status: "done", text: str(d.text) };
      if (raw) row.raw = raw;
      const i = t.steps.findIndex((s) => s.id === id);
      if (i < 0) return { ...t, steps: [...t.steps, row] };
      const steps = t.steps.slice();
      steps[i] = row;
      return { ...t, steps };
    }
    case "ask_user":
      return applyAskUser(t, d, ev.seq);
    case "todo_updated":
      return { ...t, todo: toTodo(d.items) };
    case "subtopic":
      return applySubtopic(t, d);
    case "tool_call":
      return applyToolCall(t, d, ev.seq);
    case "tool_result":
      return applyToolResult(t, d, ev.seq);
    case "budget":
      return { ...t, budget: { used: num(d.used), limit: num(d.limit) } };
    case "assistant_delta": {
      const mid = str(d.message_id);
      const sep = t.reply && t.messageId !== undefined && mid !== t.messageId ? "\n\n" : "";
      return { ...t, reply: t.reply + sep + str(d.text), messageId: mid };
    }
    case "report_ready": {
      const report: ReportRef = {
        artifactId: str(d.artifact_id),
        version: num(d.version),
        title: str(d.title) || "研究报告",
        partial: d.partial === true,
        toolBudgetReached: d.tool_budget_reached === true,
      };
      const note = optStr(d.note);
      if (note) report.note = note;
      return { ...t, report };
    }
    case "turn_stopped": {
      const card = isRecord(d.card) ? d.card : {};
      const todo = card.todo !== undefined ? toTodo(card.todo) : t.todo;
      const stop: StopInfo = {
        todo,
        subtopicsDone: num(card.subtopics_done),
        subtopicsTotal: num(card.subtopics_total),
        sources: num(card.sources),
        budget: { used: num(card.tool_calls_used), limit: num(card.tool_call_limit) },
        findings: str(d.findings),
        canFinish: d.can_finish === true,
      };
      return { ...t, stop, todo };
    }
    case "turn_result":
      return t.reply ? t : { ...t, reply: str(d.summary) };
    default:
      return t;
  }
}

const TURN_EVENTS: ReadonlySet<string> = new Set([
  "turn_created", "turn_status", "route", "skill_read", "thinking", "ask_user", "todo_updated", "subtopic",
  "tool_call", "tool_result", "budget", "assistant_delta", "report_ready", "turn_stopped", "turn_result",
]);

/** 应用一条会话事件。seq ≤ cursor 的重放事件返回同一个状态对象。 */
export function applyEvent(state: ChatState, ev: SessionEvent): ChatState {
  if (typeof ev?.seq !== "number" || ev.seq <= state.cursor) return state;
  const base: ChatState = { ...state, cursor: ev.seq };

  if (ev.type === "session_state") {
    const d: Data = isRecord(ev.data) ? (ev.data as Data) : {};
    return { ...base, sessionStatus: str(d.state) || state.sessionStatus, sessionMessage: optStr(d.user_message) };
  }
  if (!ev.turn_id || !TURN_EVENTS.has(ev.type)) return base;

  const turnId = ev.turn_id;
  let turns = state.turns.slice();
  let i = turns.findIndex((t) => t.turnId === turnId);
  if (i < 0) {
    turns.push(blankTurn(turnId));
    i = turns.length - 1;
  }
  const restored = restoredSet(turns);
  let next = applyTurnEvent(turns[i]!, ev, restored);
  const at = Date.parse(ev.ts);
  if (Number.isFinite(at)) next = { ...next, startedAt: next.startedAt ?? at, lastEventAt: at };
  turns[i] = next;
  if (ev.type === "turn_created") {
    // 恢复出的新一轮：源轮次不再显示"恢复"。
    if (next.restoredFrom) {
      turns = turns.map((t) => (t.turnId === next.restoredFrom && t.canRestore ? { ...t, canRestore: false } : t));
    }
    turns = sortTurns(turns);
  }
  return { ...base, turns };
}

/** 本页提交回答后记下回答文本（事件里没有回答内容）。 */
export function recordAnswers(state: ChatState, turnId: string, answers: string[]): ChatState {
  const i = state.turns.findIndex((t) => t.turnId === turnId);
  const t = state.turns[i];
  if (!t?.question) return state;
  const turns = state.turns.slice();
  turns[i] = { ...t, question: { ...t.question, answers: answers.slice() } };
  return { ...state, turns };
}

/** 右侧面板跟随的轮次：最近一轮研究（已走研究路径，或强制研究且尚未定路径）。 */
export function activeResearchTurn(state: ChatState): TurnView | undefined {
  for (let i = state.turns.length - 1; i >= 0; i--) {
    const t = state.turns[i]!;
    if (t.route === "research" || (t.route === undefined && (t.deepResearch || t.restoredFrom))) return t;
  }
  return undefined;
}

/**
 * 轮次的路径标签。恢复出的轮次由旧版 Worker 运行时没有 route 事件（源轮次的 route 属于源轮次）：
 * 可恢复的是被停止或被取代的研究，标为"已恢复的研究"。
 */
export function routeLabel(t: TurnView): string | undefined {
  if (t.route === "research") return "深度研究";
  if (t.route === "answer") return "直接回答";
  return t.restoredFrom ? "已恢复的研究" : undefined;
}

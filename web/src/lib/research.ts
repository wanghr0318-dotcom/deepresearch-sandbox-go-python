// DeepResearch 助手页的展示逻辑：状态文案、阶段进度、证据计数、列表标题与用户可见的错误文案。
// 用户只看到主题、状态、阶段进度、证据数量与报告；从不展示费用、模型、预算或调用明细。

import { ApiError } from "../api/client";
import type { Task, TaskEvent } from "../api/client";
import { TERMINAL_EVENT } from "../api/sse";

export const STATUS_LABELS: Record<string, string> = {
  queued: "排队中",
  running: "研究中",
  pausing: "暂停中",
  paused: "已暂停",
  cancelling: "正在取消",
  cancelled: "已取消",
  succeeded: "已完成",
  failed: "未完成",
};

export function statusLabel(status: string | undefined | null): string {
  return (status && STATUS_LABELS[status]) || "处理中";
}

const TERMINAL = new Set(["succeeded", "failed", "cancelled"]);

export function isTerminalStatus(status: string | undefined | null): boolean {
  return !!status && TERMINAL.has(status);
}

/** 研究是否仍在进行（用于"有研究进行中"提示与列表自动刷新）。 */
export function isActive(t: Pick<Task, "status">): boolean {
  return !isTerminalStatus(t.status);
}

/** 用户可以取消：未结束，且尚未请求取消。 */
export function canCancel(t: Pick<Task, "status" | "desired"> | null): boolean {
  return !!t && !isTerminalStatus(t.status) && t.desired !== "cancel" && t.status !== "cancelling";
}

// ---- 阶段进度：plan → 子任务 → report ----

export type StageState = "pending" | "active" | "done" | "stopped";

export interface Progress {
  plan: StageState;
  tasks: StageState;
  report: StageState;
  /** 已完成（已提交 checkpoint）的子任务数。 */
  subtasksDone: number;
}

const PLAN_STEP = "plan";
const REPORT_STEP = "report";
const TASK_STEP = /^task-/;

function terminalStatusOf(events: TaskEvent[]): string | undefined {
  for (let i = events.length - 1; i >= 0; i--) {
    const e = events[i]!;
    if (e.type === TERMINAL_EVENT) {
      const s = e.payload?.task_status;
      return typeof s === "string" ? s : undefined;
    }
  }
  return undefined;
}

/**
 * 由（已脱敏的）事件流与任务状态推导阶段进度。DeepResearch 每完成一步提交一个 checkpoint，
 * checkpoint_committed.step_id 依次为 plan、task-<n>…、report。
 */
export function deriveProgress(events: TaskEvent[], status: string | undefined): Progress {
  const steps = new Set<string>();
  for (const e of events) {
    if (e.source === "host" && e.type === "checkpoint_committed") {
      const id = e.payload?.step_id;
      if (typeof id === "string") steps.add(id);
    }
  }
  const subtasksDone = [...steps].filter((s) => TASK_STEP.test(s)).length;
  const finalStatus = isTerminalStatus(status) ? status : (terminalStatusOf(events) ?? status);

  const done = [steps.has(PLAN_STEP) || subtasksDone > 0 || steps.has(REPORT_STEP), steps.has(REPORT_STEP), steps.has(REPORT_STEP)];
  // 子任务阶段：报告开始（或完成）即视为子任务全部完成。
  let states: StageState[];
  if (finalStatus === "succeeded") {
    states = ["done", "done", "done"];
  } else {
    const firstOpen = done.findIndex((d) => !d);
    const stoppedLike = finalStatus === "failed" || finalStatus === "cancelled";
    const running = !stoppedLike && finalStatus !== "queued" && finalStatus !== undefined;
    states = done.map((d, i) => {
      if (d) return "done";
      if (i !== firstOpen) return "pending";
      if (stoppedLike) return "stopped";
      return running ? "active" : "pending";
    });
  }
  return { plan: states[0]!, tasks: states[1]!, report: states[2]!, subtasksDone };
}

// ---- 报告解析 ----

/** 报告末尾"## 证据"一节中的条目数（"- [n] 标题 — URL"）。 */
export function countEvidence(markdown: string): number {
  const m = /^#{1,6}\s*证据\s*$/m.exec(markdown);
  if (!m) return 0;
  const rest = markdown.slice(m.index + m[0].length);
  const next = /^#{1,6}\s/m.exec(rest);
  const section = next ? rest.slice(0, next.index) : rest;
  return section.split(/\r?\n/).filter((l) => /^\s*[-*]\s*\[\d+\]/.test(l)).length;
}

/** 报告的一级标题（DeepResearch 用研究主题作标题）。 */
export function reportTitle(markdown: string): string {
  const m = /^#\s+(.+?)\s*$/m.exec(markdown);
  return m?.[1] ?? "";
}

/** 下载文件名：主题（去掉文件系统不允许的字符）+ .md。 */
export function reportFilename(topic: string, taskId: string): string {
  const base = [...topic.replace(/[\\/:*?"<>|\0\r\n\t]+/g, " ").trim()].slice(0, 60).join("").trim();
  return `${base || `研究报告-${taskId.slice(0, 8)}`}.md`;
}

// ---- 列表展示 ----

/** 研究标题：服务端任务视图中的主题（spec.topic）；缺失时用 ID 前缀。 */
export function researchTitle(t: Pick<Task, "task_id" | "topic">): string {
  return t.topic?.trim() || `研究 ${t.task_id.slice(0, 8)}`;
}

/** 创建时间的本地简短格式：今年内 "10月6日 16:30"，否则带年份。 */
export function formatCreated(ts: string | undefined | null, now: Date = new Date()): string {
  if (!ts) return "";
  const d = new Date(ts);
  if (Number.isNaN(d.getTime())) return "";
  const hm = `${String(d.getHours()).padStart(2, "0")}:${String(d.getMinutes()).padStart(2, "0")}`;
  const md = `${d.getMonth() + 1}月${d.getDate()}日 ${hm}`;
  return d.getFullYear() === now.getFullYear() ? md : `${d.getFullYear()}年${md}`;
}

export function isUnauthorized(e: unknown): boolean {
  return e instanceof ApiError && e.status === 401;
}

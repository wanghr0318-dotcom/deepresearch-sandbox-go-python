// 事件时间线：按事件 ID（task_seq）去重并排序；按 sub-run 分泳道。
// M4 之前事件不带 sub-run 信息，全部落在 root 一条泳道。
import type { TaskEvent } from "../api/client";

export const ROOT_LANE = "root";

/** 合并新到事件：同一 task_seq 只保留先到的一条，结果按 task_seq 升序。 */
export function mergeEvents(existing: readonly TaskEvent[], incoming: readonly TaskEvent[]): TaskEvent[] {
  const bySeq = new Map<number, TaskEvent>();
  for (const ev of existing) bySeq.set(ev.task_seq, ev);
  for (const ev of incoming) {
    if (!Number.isSafeInteger(ev.task_seq) || bySeq.has(ev.task_seq)) continue;
    bySeq.set(ev.task_seq, ev);
  }
  return [...bySeq.values()].sort((a, b) => a.task_seq - b.task_seq);
}

/** 事件所属泳道：payload 带 subrun_id 时按它分组，否则为 root。 */
export function laneOf(ev: TaskEvent): string {
  const id = ev.payload?.["subrun_id"];
  return typeof id === "string" && id !== "" ? id : ROOT_LANE;
}

export interface Lane {
  id: string;
  events: TaskEvent[];
}

/** 把有序事件分到泳道；root 永远在第一位且总是存在。 */
export function lanes(events: readonly TaskEvent[]): Lane[] {
  const out = new Map<string, TaskEvent[]>([[ROOT_LANE, []]]);
  for (const ev of events) {
    const id = laneOf(ev);
    let list = out.get(id);
    if (!list) {
      list = [];
      out.set(id, list);
    }
    list.push(ev);
  }
  return [...out.entries()].map(([id, evs]) => ({ id, events: evs }));
}

/** 宿主事件与 Worker 事件的区分（source 字段）。 */
export function sourceKind(ev: TaskEvent): "host" | "worker" | "other" {
  if (ev.source === "host") return "host";
  if (ev.source === "worker") return "worker";
  return "other";
}

const SUMMARY_KEYS = [
  "message",
  "step_id",
  "artifact_id",
  "version",
  "media_type",
  "checkpoint_id",
  "desired",
  "status",
  "reason",
  "outcome_class",
  "attempt_no",
];

/** 事件的一行摘要（不展开整个 payload）。 */
export function eventSummary(ev: TaskEvent): string {
  const p = ev.payload ?? {};
  const parts: string[] = [];
  for (const k of SUMMARY_KEYS) {
    const v = p[k];
    if (typeof v === "string" || typeof v === "number" || typeof v === "boolean") parts.push(`${k}=${String(v)}`);
  }
  return parts.join(" · ");
}

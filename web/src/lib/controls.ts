// 控制按钮的启用条件。服务端仍是权威（拒绝时返回 task_ended / cancel_pending / not_paused），
// 这里只按服务端返回的最新 Task 事实避免显然会被拒绝的操作（与 internal/task/control.go 的转换表一致）。
import type { Task } from "../api/client";

export const TERMINAL_STATUSES = new Set(["succeeded", "failed", "cancelled"]);

export function isTerminal(status: string): boolean {
  return TERMINAL_STATUSES.has(status);
}

export interface ControlAvailability {
  cancel: boolean;
  pause: boolean;
  resume: boolean;
}

export function controlAvailability(task: Pick<Task, "status" | "desired"> | null | undefined): ControlAvailability {
  if (!task || isTerminal(task.status) || task.desired === "cancel") {
    return { cancel: false, pause: false, resume: false };
  }
  return {
    cancel: true,
    pause: task.desired === "run" && (task.status === "queued" || task.status === "running"),
    resume: task.status === "paused",
  };
}

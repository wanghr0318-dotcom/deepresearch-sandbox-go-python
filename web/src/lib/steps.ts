// 步骤行的展示分组（纯函数）：同一分组内连续的"思考"合并为一行，连续的"阅读网页"合并为一行；
// 其余行原样保留。只影响展示，不改事件归约的状态。
import type { StepRow, TurnView } from "./chat";

export type StepItem =
  | { kind: "row"; row: StepRow }
  | { kind: "merged"; id: string; rowKind: "thinking" | "fetch"; title: string; rows: StepRow[] };

const MERGEABLE = new Set<StepRow["kind"]>(["thinking", "fetch"]);

function mergedTitle(kind: "thinking" | "fetch", n: number): string {
  return kind === "thinking" ? `思考 · ${n} 次` : `阅读网页 · ${n} 个网页`;
}

export function groupSteps(rows: StepRow[]): StepItem[] {
  const out: StepItem[] = [];
  let run: StepRow[] = [];
  const flush = (): void => {
    if (run.length === 1) out.push({ kind: "row", row: run[0]! });
    else if (run.length > 1) {
      const k = run[0]!.kind as "thinking" | "fetch";
      out.push({ kind: "merged", id: `merged-${run[0]!.id}`, rowKind: k, title: mergedTitle(k, run.length), rows: run });
    }
    run = [];
  };
  for (const row of rows) {
    if (MERGEABLE.has(row.kind) && (run.length === 0 || run[0]!.kind === row.kind)) {
      run.push(row);
      continue;
    }
    flush();
    if (MERGEABLE.has(row.kind)) run.push(row);
    else out.push({ kind: "row", row });
  }
  flush();
  return out;
}

const FINISHED = new Set(["succeeded", "failed", "cancelled", "paused"]);

export function turnFinished(t: Pick<TurnView, "status">): boolean {
  return FINISHED.has(t.status);
}

export function durationText(ms: number): string {
  const s = Math.max(0, Math.round(ms / 1000));
  const m = Math.floor(s / 60);
  return m > 0 ? `${m} 分 ${s % 60} 秒` : `${s} 秒`;
}

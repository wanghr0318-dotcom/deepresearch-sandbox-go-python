// 组件测试辅助：mock 的 ApiClient、事件流与保存文件（只在 *.test.ts 中使用，不进入构建产物）。
import { vi } from "vitest";
import type { Task, TaskEvent } from "../api/client";
import type { EventStream, StreamOutcome, WatchOptions } from "../api/sse";
import type { ApiLike, Services } from "../lib/services";
import { servicesKey } from "../lib/services";

export function task(over: Partial<Task> = {}): Task {
  return {
    task_id: "t-1",
    status: "running",
    desired: "run",
    control_version: 0,
    applied_control_version: 0,
    attempts_total: 1,
    ...over,
  };
}

export function ev(seq: number, type: string, over: Partial<TaskEvent> = {}): TaskEvent {
  return { task_seq: seq, source: "host", type, ts: "2026-10-05T00:00:00Z", ...over };
}

type Mocked<T> = { [K in keyof T]: T[K] & ReturnType<typeof vi.fn> };

export function fakeApi(over: Partial<ApiLike> = {}): Mocked<ApiLike> {
  const base: ApiLike = {
    getStatus: vi.fn(async () => ({ mode: "normal" as const })),
    listTasks: vi.fn(async () => ({ tasks: [] })),
    getTask: vi.fn(async (id: string) => task({ task_id: id })),
    createTask: vi.fn(async () => ({ task_id: "t-new" })),
    cancelTask: vi.fn(async (id: string) => ({ task_id: id, control_version: 1 })),
    pauseTask: vi.fn(async (id: string) => ({ task_id: id, control_version: 1 })),
    resumeTask: vi.fn(async (id: string) => ({ task_id: id, control_version: 1 })),
    getResult: vi.fn(async () => ({ summary: "", outputs: [] })),
    inspectTask: vi.fn(async (id: string) => ({ task: task({ task_id: id }), attempts: [], checkpoints: [], calls: [] })),
    downloadArtifact: vi.fn(async () => ({ blob: new Blob([""]), contentType: "text/plain", etag: "", contentDisposition: "" })),
  };
  for (const [k, fn] of Object.entries(over)) {
    (base as unknown as Record<string, unknown>)[k] = vi.isMockFunction(fn) ? fn : vi.fn(fn as (...a: unknown[]) => unknown);
  }
  return base as Mocked<ApiLike>;
}

/** 可手动推送事件的假事件流。 */
export interface FakeWatch {
  fn: Services["watch"] & ReturnType<typeof vi.fn>;
  opts: () => Pick<WatchOptions, "taskId" | "cursor" | "onEvent" | "onState">;
  emit: (e: TaskEvent) => void;
  finish: (o: StreamOutcome) => void;
  closed: () => boolean;
}

export function fakeWatch(): FakeWatch {
  let last: Pick<WatchOptions, "taskId" | "cursor" | "onEvent" | "onState"> | undefined;
  let resolve: (o: StreamOutcome) => void = () => {};
  let closed = false;
  const fn = vi.fn((opts: Pick<WatchOptions, "taskId" | "cursor" | "onEvent" | "onState">): EventStream => {
    last = opts;
    const done = new Promise<StreamOutcome>((r) => {
      resolve = r;
    });
    opts.onState?.("open", { attempt: 0 });
    return {
      close: () => {
        closed = true;
      },
      get cursor() {
        return 0;
      },
      done,
    };
  });
  return {
    fn,
    opts: () => {
      if (!last) throw new Error("watch not called");
      return last;
    },
    emit: (e) => last?.onEvent(e, e.type),
    finish: (o) => resolve(o),
    closed: () => closed,
  };
}

export function services(api: ApiLike, watch?: Services["watch"], saveBlob?: Services["saveBlob"]) {
  const s: Services = {
    api,
    watch: watch ?? fakeWatch().fn,
    saveBlob: saveBlob ?? vi.fn(),
  };
  return { provide: { [servicesKey as symbol]: s } };
}

export async function flushAll(): Promise<void> {
  for (let i = 0; i < 5; i++) await new Promise((r) => setTimeout(r, 0));
}

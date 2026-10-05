import { mount } from "@vue/test-utils";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ApiError } from "../api/client";
import { ev, fakeApi, fakeWatch, flushAll, services, task } from "../components/testkit";
import TaskDetailView from "./TaskDetailView.vue";
import TaskListView from "./TaskListView.vue";

afterEach(() => {
  window.location.hash = "";
});

describe("TaskListView", () => {
  it("pages with the server cursor (next → after) and back", async () => {
    const listTasks = vi.fn(async (p: { after?: string; limit?: number } = {}) => {
      if (!p.after) return { tasks: [task({ task_id: "t-1", status: "succeeded" }), task({ task_id: "t-2" })], next: "c1" };
      if (p.after === "c1") return { tasks: [task({ task_id: "t-3", status: "failed" })], next: "c2" };
      return { tasks: [task({ task_id: "t-4", status: "paused" })] };
    });
    const w = mount(TaskListView, { props: { pageSize: 2 }, global: services(fakeApi({ listTasks })) });
    await flushAll();
    expect(listTasks).toHaveBeenLastCalledWith({ limit: 2 });
    expect(w.findAll("tr[data-task]").map((r) => r.attributes("data-task"))).toEqual(["t-1", "t-2"]);
    expect(w.get('tr[data-task="t-1"] .badge').text()).toBe("succeeded");
    expect(w.get('tr[data-task="t-1"] .badge').classes()).toContain("succeeded");
    expect(w.get('[data-action="prev"]').attributes("disabled")).toBeDefined();

    await w.get('[data-action="next"]').trigger("click");
    await flushAll();
    expect(listTasks).toHaveBeenLastCalledWith({ after: "c1", limit: 2 });
    expect(w.findAll("tr[data-task]").map((r) => r.attributes("data-task"))).toEqual(["t-3"]);

    await w.get('[data-action="next"]').trigger("click");
    await flushAll();
    expect(listTasks).toHaveBeenLastCalledWith({ after: "c2", limit: 2 });
    expect(w.get('[data-action="next"]').attributes("disabled")).toBeDefined(); // 最后一页没有 next

    await w.get('[data-action="prev"]').trigger("click");
    await flushAll();
    expect(listTasks).toHaveBeenLastCalledWith({ after: "c1", limit: 2 });

    await w.get('[data-action="first"]').trigger("click");
    await flushAll();
    expect(listTasks).toHaveBeenLastCalledWith({ limit: 2 });
  });

  it("links tasks with hash routes, not API paths", async () => {
    const api = fakeApi({ listTasks: vi.fn(async () => ({ tasks: [task({ task_id: "t/1" })] })) });
    const w = mount(TaskListView, { global: services(api) });
    await flushAll();
    expect(w.get("tr[data-task] a").attributes("href")).toBe("#/admin/tasks/t%2F1");
  });

  it("shows a diagnostic_mode error specifically", async () => {
    const api = fakeApi({ listTasks: vi.fn().mockRejectedValue(new ApiError(503, "diagnostic_mode", "")) });
    const w = mount(TaskListView, { global: services(api) });
    await flushAll();
    expect(w.get(".error-banner").text()).toContain("诊断模式");
  });

  it("navigates to the new task after submit", async () => {
    const w = mount(TaskListView, { global: services(fakeApi()) });
    await flushAll();
    await w.get('[data-action="new"]').trigger("click");
    await w.get("form").trigger("submit");
    await flushAll();
    expect(window.location.hash).toBe("#/admin/tasks/t-new");
  });
});

describe("TaskDetailView", () => {
  it("builds the timeline from the event stream: dedup by id, ordered by task_seq, host/worker distinguished", async () => {
    const watch = fakeWatch();
    const w = mount(TaskDetailView, { props: { id: "t-1" }, global: services(fakeApi(), watch.fn) });
    await flushAll();
    expect(watch.opts().taskId).toBe("t-1");
    expect(watch.opts().cursor).toBe(0);
    watch.emit(ev(2, "attempt_created"));
    watch.emit(ev(1, "task_created"));
    watch.emit(ev(3, "progress", { source: "worker", worker_seq: 1, payload: { message: "step 1" } }));
    watch.emit(ev(2, "attempt_created_dup"));
    watch.emit(ev(3, "progress_dup", { source: "worker" }));
    await flushAll();
    const rows = w.findAll("li.event");
    expect(rows.map((r) => r.attributes("data-seq"))).toEqual(["1", "2", "3"]);
    expect(rows[1]!.text()).toContain("attempt_created");
    expect(rows[1]!.text()).not.toContain("dup");
    expect(rows[2]!.classes()).toContain("worker");
    expect(rows[0]!.classes()).toContain("host");
    expect(rows[2]!.text()).toContain("message=step 1");
    expect(w.findAll("section.lane").map((l) => l.attributes("data-lane"))).toEqual(["root"]);
    w.unmount();
    expect(watch.closed()).toBe(true);
  });

  it("enables controls from the server task and refreshes it after events", async () => {
    const getTask = vi
      .fn()
      .mockResolvedValueOnce(task({ status: "running" }))
      .mockResolvedValue(task({ status: "paused", desired: "pause" }));
    const watch = fakeWatch();
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    try {
      const w = mount(TaskDetailView, { props: { id: "t-1" }, global: services(fakeApi({ getTask }), watch.fn) });
      await vi.runAllTimersAsync();
      expect(w.get('[data-action="pause"]').attributes("disabled")).toBeUndefined();
      expect(w.get('[data-action="resume"]').attributes("disabled")).toBeDefined();
      watch.emit(ev(5, "control_applied", { payload: { desired: "pause" } }));
      await vi.runAllTimersAsync();
      expect(getTask).toHaveBeenCalledTimes(2);
      expect(w.get('[data-action="resume"]').attributes("disabled")).toBeUndefined();
      expect(w.get('[data-action="pause"]').attributes("disabled")).toBeDefined();
      w.unmount();
    } finally {
      vi.useRealTimers();
    }
  });

  it("loads the pinned result on task_terminal and shows the artifacts", async () => {
    const getTask = vi.fn().mockResolvedValueOnce(task({ status: "running" })).mockResolvedValue(task({ status: "succeeded" }));
    const getResult = vi.fn(async () => ({ summary: "研究完成", outputs: [{ artifact_id: "report", version: 1, sha256: "a".repeat(64) }] }));
    const watch = fakeWatch();
    const w = mount(TaskDetailView, { props: { id: "t-1" }, global: services(fakeApi({ getTask, getResult }), watch.fn) });
    await flushAll();
    watch.emit(ev(1, "artifact_saved", { payload: { artifact_id: "report", version: 1, sha256: "a".repeat(64) } }));
    watch.emit(ev(2, "task_terminal", { payload: { status: "succeeded" } }));
    await flushAll();
    expect(getResult).toHaveBeenCalledWith("t-1");
    await w.get('[data-tab="artifacts"]').trigger("click");
    expect(w.text()).toContain("研究完成");
    expect(w.find('tr[data-artifact="report"] .badge.pinned').exists()).toBe(true);
    // 终态：控制全部禁用
    for (const a of ["pause", "resume", "cancel"]) expect(w.get(`[data-action="${a}"]`).attributes("disabled")).toBeDefined();
    w.unmount();
  });

  it("shows the raw inspect view and the call details", async () => {
    const inspectTask = vi.fn(async () => ({
      task: task(),
      attempts: [{ attempt_id: "a-1", status: "running" }],
      checkpoints: [],
      calls: [],
    }));
    const w = mount(TaskDetailView, { props: { id: "t-1" }, global: services(fakeApi({ inspectTask }), fakeWatch().fn) });
    await flushAll();
    await w.get('[data-tab="inspect"]').trigger("click");
    await flushAll();
    expect(inspectTask).toHaveBeenCalledWith("t-1");
    const raw = w.get('[data-testid="inspect-raw"]').text();
    expect(JSON.parse(raw)).toEqual(await inspectTask.mock.results[0]!.value);
    await w.get('[data-tab="calls"]').trigger("click");
    expect(w.text()).toContain("没有 Gateway 调用记录");
    w.unmount();
  });

  it("reports a stopped stream (invalid_cursor) and ownership_lost on the task", async () => {
    const watch = fakeWatch();
    const api = fakeApi({ getTask: vi.fn().mockRejectedValue(new ApiError(503, "ownership_lost", "")) });
    const w = mount(TaskDetailView, { props: { id: "t-1" }, global: services(api, watch.fn) });
    await flushAll();
    watch.finish({ kind: "error", lastSeq: 0, error: new ApiError(400, "invalid_cursor", "beyond") });
    await flushAll();
    const text = w.findAll(".error-banner").map((b) => b.text()).join("\n");
    expect(text).toContain("所有权");
    expect(text).toContain("事件游标无效");
    w.unmount();
  });
});

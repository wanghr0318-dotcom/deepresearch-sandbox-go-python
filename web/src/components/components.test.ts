import { mount } from "@vue/test-utils";
import { describe, expect, it, vi } from "vitest";
import { ApiError, NetworkError } from "../api/client";
import type { TaskEvent } from "../api/client";
import ArtifactsPanel from "./ArtifactsPanel.vue";
import CallsPanel from "./CallsPanel.vue";
import ErrorBanner from "./ErrorBanner.vue";
import SubmitTaskForm from "./SubmitTaskForm.vue";
import TaskControls from "./TaskControls.vue";
import { ev, fakeApi, flushAll, services, task } from "./testkit";

function btn(w: ReturnType<typeof mount>, action: string) {
  return w.get(`button[data-action="${action}"]`);
}

describe("TaskControls (E27: buttons follow server state)", () => {
  const cases: [string, "run" | "pause" | "cancel", string[]][] = [
    ["queued", "run", ["pause", "cancel"]],
    ["running", "run", ["pause", "cancel"]],
    ["pausing", "pause", ["cancel"]],
    ["paused", "pause", ["resume", "cancel"]],
    ["cancelling", "cancel", []],
    ["succeeded", "run", []],
    ["failed", "run", []],
    ["cancelled", "cancel", []],
  ];
  it.each(cases)("status=%s desired=%s enables %j", (status, desired, enabled) => {
    const w = mount(TaskControls, { props: { task: task({ status, desired }) }, global: services(fakeApi()) });
    for (const a of ["pause", "resume", "cancel"]) {
      expect(btn(w, a).attributes("disabled") === undefined, a).toBe(enabled.includes(a));
    }
  });

  it("updates when the server task changes", async () => {
    const w = mount(TaskControls, { props: { task: task({ status: "running" }) }, global: services(fakeApi()) });
    expect(btn(w, "resume").attributes("disabled")).toBeDefined();
    await w.setProps({ task: task({ status: "paused", desired: "pause" }) });
    expect(btn(w, "resume").attributes("disabled")).toBeUndefined();
    expect(btn(w, "pause").attributes("disabled")).toBeDefined();
  });

  it("sends the control and reuses request_id after an unknown-outcome failure", async () => {
    const pauseTask = vi
      .fn()
      .mockRejectedValueOnce(new NetworkError("down"))
      .mockResolvedValueOnce({ task_id: "t-1", control_version: 2 });
    const api = fakeApi({ pauseTask });
    const w = mount(TaskControls, { props: { task: task() }, global: services(api) });
    await btn(w, "pause").trigger("click");
    await flushAll();
    expect(w.find(".error-banner").exists()).toBe(true);
    await btn(w, "pause").trigger("click");
    await flushAll();
    expect(pauseTask).toHaveBeenCalledTimes(2);
    expect(pauseTask.mock.calls[0]![0]).toBe("t-1");
    expect(pauseTask.mock.calls[1]![2]).toBe(pauseTask.mock.calls[0]![2]);
    expect(w.emitted("changed")).toHaveLength(1);
  });

  it("shows the server rejection (cancel_pending) clearly", async () => {
    const api = fakeApi({
      cancelTask: vi.fn().mockRejectedValue(new ApiError(409, "cancel_pending", "cancel already accepted")),
    });
    const w = mount(TaskControls, { props: { task: task() }, global: services(api) });
    await btn(w, "cancel").trigger("click");
    await flushAll();
    expect(w.get(".error-banner").text()).toContain("已接受取消");
    expect(w.get(".error-banner").attributes("data-code")).toBe("cancel_pending");
  });
});

describe("ErrorBanner", () => {
  it("shows diagnostic_mode / ownership_lost / budget specifically", () => {
    for (const [code, text] of [
      ["diagnostic_mode", "诊断模式"],
      ["ownership_lost", "所有权"],
      ["budget_insufficient_for_request", "剩余预算不足"],
    ] as const) {
      const w = mount(ErrorBanner, { props: { error: new ApiError(503, code, "") } });
      expect(w.text()).toContain(text);
      expect(w.text()).toContain(code);
    }
    expect(mount(ErrorBanner, { props: { error: null } }).find(".error-banner").exists()).toBe(false);
  });
});

describe("SubmitTaskForm", () => {
  it("sends spec and limits; reuses request_id on retry; new id after the content changes", async () => {
    const createTask = vi
      .fn()
      .mockRejectedValueOnce(new ApiError(503, "commit_unknown", "unknown"))
      .mockRejectedValueOnce(new ApiError(503, "contention", ""))
      .mockResolvedValueOnce({ task_id: "t-42" });
    const w = mount(SubmitTaskForm, { global: services(fakeApi({ createTask })) });
    await w.get("textarea").setValue('{"summary":"x"}');
    await w.get('input[name="budget_micro"]').setValue("250000");
    await w.get("form").trigger("submit");
    await flushAll();
    expect(w.get(".error-banner").text()).toContain("提交结果未知");
    expect(w.get("button[type=submit]").text()).toContain("同一 request_id");

    await w.get("form").trigger("submit");
    await flushAll();
    const [first, second] = createTask.mock.calls;
    expect(first![0]).toEqual({ spec: { summary: "x" }, limits: { budget_micro: 250000 } });
    expect(second![1]).toBe(first![1]);

    await w.get("textarea").setValue('{"summary":"y"}');
    await w.get("form").trigger("submit");
    await flushAll();
    const third = createTask.mock.calls[2]!;
    expect(third[1]).not.toBe(first![1]);
    expect(third[0]).toEqual({ spec: { summary: "y" }, limits: { budget_micro: 250000 } });
    expect(w.emitted("created")).toEqual([["t-42"]]);
  });

  it("rejects invalid JSON and non-integer limits without calling the API", async () => {
    const api = fakeApi();
    const w = mount(SubmitTaskForm, { global: services(api) });
    await w.get("textarea").setValue("{nope");
    await w.get("form").trigger("submit");
    expect(w.text()).toContain("不是合法 JSON");
    await w.get("textarea").setValue("[]");
    await w.get("form").trigger("submit");
    expect(w.text()).toContain("必须是 JSON 对象");
    await w.get("textarea").setValue("{}");
    await w.get('input[name="max_run_time_ms"]').setValue("-5");
    await w.get("form").trigger("submit");
    expect(w.text()).toContain("max_run_time_ms 必须是非负整数");
    expect(api.createTask).not.toHaveBeenCalled();
  });

  it("omits limits when all are empty", async () => {
    const api = fakeApi();
    const w = mount(SubmitTaskForm, { global: services(api) });
    await w.get("textarea").setValue('{"a":1}');
    await w.get("form").trigger("submit");
    await flushAll();
    expect(api.createTask.mock.calls[0]![0]).toEqual({ spec: { a: 1 } });
  });
});

function artifactEvents(): TaskEvent[] {
  const declare = (seq: number, id: string, media: string, visibility = "output") =>
    ev(seq, "artifact", { source: "worker", payload: { artifact_id: id, media_type: media, visibility, path: `${id}.x` } });
  const saved = (seq: number, id: string, version: number) =>
    ev(seq, "artifact_saved", { payload: { artifact_id: id, version, sha256: `${id}${version}`.padEnd(64, "0") } });
  return [
    declare(1, "report", "text/markdown"),
    saved(2, "report", 1),
    declare(3, "page", "text/html"),
    saved(4, "page", 1),
    declare(5, "chart", "image/svg+xml"),
    saved(6, "chart", 1),
    declare(7, "scratch", "text/plain", "internal"),
    saved(8, "scratch", 1),
    saved(9, "mystery", 1),
  ];
}

const HOSTILE_MD =
  '# 报告\n\n<script>window.__pwned=1</script>\n\n<img src=x onerror="window.__pwned=2">\n\n[x](javascript:window.__pwned=3)\n\n<iframe src="https://evil.example"></iframe>\n';

describe("ArtifactsPanel (E27)", () => {
  function row(w: ReturnType<typeof mount>, id: string) {
    return w.get(`tr[data-artifact="${id}"]`);
  }

  it("offers only a download button for HTML and SVG artifacts", () => {
    const w = mount(ArtifactsPanel, {
      props: { taskId: "t-1", events: artifactEvents(), result: null },
      global: services(fakeApi()),
    });
    for (const id of ["page", "chart"]) {
      expect(row(w, id).find('[data-action="download"]').exists(), id).toBe(true);
      expect(row(w, id).find('[data-action="preview"]').exists(), id).toBe(false);
    }
    expect(row(w, "report").find('[data-action="preview"]').exists()).toBe(true);
    // internal 产物：不可下载
    expect(row(w, "scratch").find("button").exists()).toBe(false);
    expect(row(w, "scratch").text()).toContain("internal");
  });

  it("downloads through the ApiClient as a Blob (never a URL with a token)", async () => {
    const blob = new Blob(["<h1>hi</h1>"], { type: "text/html" });
    const downloadArtifact = vi.fn(async () => ({
      blob,
      contentType: "text/html",
      etag: '"x"',
      contentDisposition: 'attachment; filename="page.html"',
    }));
    const saveBlob = vi.fn();
    const w = mount(ArtifactsPanel, {
      props: { taskId: "t-1", events: artifactEvents(), result: null },
      global: services(fakeApi({ downloadArtifact }), undefined, saveBlob),
    });
    await row(w, "page").get('[data-action="download"]').trigger("click");
    await flushAll();
    expect(downloadArtifact).toHaveBeenCalledWith("t-1", "page", 1);
    expect(saveBlob).toHaveBeenCalledWith(blob, "page.html");
    expect(w.find("iframe").exists()).toBe(false);
    expect(w.html()).not.toContain("<h1>hi</h1>");
    expect(w.findAll("a").filter((a) => (a.attributes("href") ?? "").includes("/tasks/"))).toHaveLength(0);
  });

  it("renders a hostile markdown report without executable content", async () => {
    const downloadArtifact = vi.fn(async () => ({
      blob: new Blob([HOSTILE_MD], { type: "text/markdown" }),
      contentType: "text/markdown; charset=utf-8",
      etag: "",
      contentDisposition: "",
    }));
    const w = mount(ArtifactsPanel, {
      props: { taskId: "t-1", events: artifactEvents(), result: null },
      global: services(fakeApi({ downloadArtifact })),
      attachTo: document.body,
    });
    await row(w, "report").get('[data-action="preview"]').trigger("click");
    await flushAll();
    const md = w.get(".md-preview");
    expect(md.find("h1").text()).toBe("报告");
    expect(md.find("script").exists()).toBe(false);
    expect(md.find("iframe").exists()).toBe(false);
    for (const el of md.findAll("*")) {
      for (const attr of Array.from(el.element.attributes)) {
        expect(attr.name.startsWith("on")).toBe(false);
        expect(attr.value.toLowerCase()).not.toContain("javascript:");
      }
    }
    for (const el of md.findAll("*")) el.element.dispatchEvent(new Event("error"));
    expect((window as unknown as { __pwned?: number }).__pwned).toBeUndefined();
    w.unmount();
  });

  it("falls back to download-only when the response is active content, even if undeclared", async () => {
    const downloadArtifact = vi.fn(async () => ({
      blob: new Blob(['<svg onload="window.__pwned=9"></svg>'], { type: "image/svg+xml" }),
      contentType: "image/svg+xml",
      etag: "",
      contentDisposition: 'attachment; filename="m.svg"',
    }));
    const w = mount(ArtifactsPanel, {
      props: { taskId: "t-1", events: artifactEvents(), result: null },
      global: services(fakeApi({ downloadArtifact })),
    });
    await row(w, "mystery").get('[data-action="preview"]').trigger("click");
    await flushAll();
    expect(w.get('[data-testid="download-only"]').text()).toContain("image/svg+xml");
    expect(w.find("svg").exists()).toBe(false);
    expect(w.find(".md-preview").exists()).toBe(false);
  });

  it("marks pinned versions from the result and lists versions newest first", () => {
    const events = [...artifactEvents(), ev(10, "artifact_saved", { payload: { artifact_id: "report", version: 2, sha256: "f".repeat(64) } })];
    const w = mount(ArtifactsPanel, {
      props: { taskId: "t-1", events, result: { summary: "done", outputs: [{ artifact_id: "report", version: 2, sha256: "f".repeat(64) }] } },
      global: services(fakeApi()),
    });
    const versions = row(w, "report").findAll("[data-version]");
    expect(versions.map((v) => v.attributes("data-version"))).toEqual(["2", "1"]);
    expect(versions[0]!.find(".badge.pinned").exists()).toBe(true);
    expect(versions[1]!.find(".badge.pinned").exists()).toBe(false);
  });

  it("shows artifact_not_found clearly", async () => {
    const downloadArtifact = vi.fn().mockRejectedValue(new ApiError(404, "artifact_not_found", "no such artifact"));
    const w = mount(ArtifactsPanel, {
      props: { taskId: "t-1", events: artifactEvents(), result: null },
      global: services(fakeApi({ downloadArtifact })),
    });
    await row(w, "report").get('[data-action="download"]').trigger("click");
    await flushAll();
    expect(w.get(".error-banner").text()).toContain("产物不存在或不可下载");
  });
});

describe("CallsPanel", () => {
  it("shows endpoint, status, cost, latency and budget failures from inspect", async () => {
    const inspection = {
      task: task(),
      attempts: [],
      checkpoints: [],
      calls: [
        {
          call_id: "c-1",
          endpoint: "chat",
          state: "completed" as const,
          source: "worker",
          first_attempt_id: "a-1",
          tries_used: 2,
          cost_charged_micro: 1234,
          possible_external_duplicate: false,
          created_at: "2026-10-05T00:00:00Z",
          deadline_at: "2026-10-05T00:01:00Z",
          tries: [
            { try_no: 1, attempt_id: "a-1", state: "settled" as const, outcome: "retryable" as const, latency_ms: 300, cost_micro: 0, error: "upstream 502" },
            { try_no: 2, attempt_id: "a-1", state: "settled" as const, outcome: "ok" as const, latency_ms: 900, cost_micro: 1234 },
          ],
        },
        {
          call_id: "c-2",
          endpoint: "search",
          state: "failed" as const,
          source: "worker",
          first_attempt_id: "a-1",
          tries_used: 0,
          cost_charged_micro: 0,
          fail_reason: "budget_exhausted",
          possible_external_duplicate: false,
          created_at: "2026-10-05T00:00:00Z",
          deadline_at: "2026-10-05T00:01:00Z",
          tries: [],
        },
      ],
    };
    const w = mount(CallsPanel, { props: { inspection } });
    expect(w.get('[data-testid="total-cost"]').text()).toBe("$0.001234");
    const c1 = w.get('tr[data-call="c-1"]');
    expect(c1.text()).toContain("chat");
    expect(c1.text()).toContain("completed");
    expect(c1.text()).toContain("1.20 s");
    expect(w.get('[data-testid="budget-failure"]').text()).toContain("budget_exhausted");
    await c1.get("button.expander").trigger("click");
    expect(w.text()).toContain("upstream 502");
  });
});

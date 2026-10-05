import { describe, expect, it } from "vitest";
import { ApiError, NetworkError } from "../api/client";
import { ev } from "../components/testkit";
import { collectArtifacts } from "./artifacts";
import { controlAvailability } from "./controls";
import { describeError } from "./errors";
import { downloadFilename, previewKindForDownload, previewKindOf } from "./media";
import { parseHash, taskHref, tasksHref } from "./router";
import { ROOT_LANE, lanes, mergeEvents } from "./timeline";

describe("mergeEvents", () => {
  it("dedups by task_seq (first wins) and orders by task_seq", () => {
    const a = [ev(1, "task_created"), ev(3, "attempt_created")];
    const merged = mergeEvents(a, [ev(2, "progress", { source: "worker" }), ev(3, "dup"), ev(1, "dup"), ev(5, "x"), ev(4, "y")]);
    expect(merged.map((e) => e.task_seq)).toEqual([1, 2, 3, 4, 5]);
    expect(merged.find((e) => e.task_seq === 3)?.type).toBe("attempt_created");
    expect(merged.find((e) => e.task_seq === 1)?.type).toBe("task_created");
  });

  it("puts everything in one root lane until sub-run data exists", () => {
    const l = lanes([ev(1, "a"), ev(2, "b", { source: "worker" })]);
    expect(l).toHaveLength(1);
    expect(l[0]!.id).toBe(ROOT_LANE);
    expect(l[0]!.events).toHaveLength(2);
    expect(lanes([])).toEqual([{ id: ROOT_LANE, events: [] }]);
    const sub = lanes([ev(1, "a"), ev(2, "subrun_started", { payload: { subrun_id: "s1" } })]);
    expect(sub.map((x) => x.id)).toEqual([ROOT_LANE, "s1"]);
  });
});

describe("controlAvailability", () => {
  const cases: [string, "run" | "pause" | "cancel", { cancel: boolean; pause: boolean; resume: boolean }][] = [
    ["queued", "run", { cancel: true, pause: true, resume: false }],
    ["running", "run", { cancel: true, pause: true, resume: false }],
    ["pausing", "pause", { cancel: true, pause: false, resume: false }],
    ["paused", "pause", { cancel: true, pause: false, resume: true }],
    ["cancelling", "cancel", { cancel: false, pause: false, resume: false }],
    ["running", "cancel", { cancel: false, pause: false, resume: false }],
    ["succeeded", "run", { cancel: false, pause: false, resume: false }],
    ["failed", "run", { cancel: false, pause: false, resume: false }],
    ["cancelled", "cancel", { cancel: false, pause: false, resume: false }],
  ];
  it.each(cases)("%s / desired=%s", (status, desired, want) => {
    expect(controlAvailability({ status, desired })).toEqual(want);
  });
  it("disables everything without a task", () => {
    expect(controlAvailability(null)).toEqual({ cancel: false, pause: false, resume: false });
  });
});

describe("media policy", () => {
  it("previews only markdown / plain text / json", () => {
    expect(previewKindOf("text/markdown; charset=utf-8")).toBe("markdown");
    expect(previewKindOf("text/plain")).toBe("text");
    expect(previewKindOf("application/json")).toBe("text");
    for (const t of ["text/html", "image/svg+xml", "application/xhtml+xml", "application/javascript", "image/png", "", "application/pdf"]) {
      expect(previewKindOf(t), t).toBe("none");
    }
  });
  it("never previews attachment responses", () => {
    expect(previewKindForDownload("text/markdown", 'attachment; filename="r.md"')).toBe("none");
    expect(previewKindForDownload("text/markdown", "")).toBe("markdown");
  });
  it("derives a safe filename", () => {
    expect(downloadFilename('attachment; filename="page.html"', "a", 1)).toBe("page.html");
    expect(downloadFilename("attachment; filename*=UTF-8''%E6%8A%A5%E5%91%8A.md", "a", 1)).toBe("报告.md");
    expect(downloadFilename('attachment; filename="../../x"', "a", 1)).toBe(".._.._x");
    expect(downloadFilename("", "report", 3)).toBe("report-v3");
  });
});

describe("collectArtifacts", () => {
  it("joins worker declarations, saved versions and pinned outputs", () => {
    const rows = collectArtifacts(
      [
        ev(1, "artifact", { source: "worker", payload: { artifact_id: "report", media_type: "text/markdown", visibility: "output", path: "r.md" } }),
        ev(2, "artifact_saved", { payload: { artifact_id: "report", version: 1, sha256: "aa" } }),
        ev(3, "artifact_saved", { payload: { artifact_id: "report", version: 2, sha256: "bb" } }),
        ev(4, "artifact", { source: "worker", payload: { artifact_id: "scratch", media_type: "text/plain", visibility: "internal" } }),
      ],
      { summary: "", outputs: [{ artifact_id: "report", version: 2, sha256: "bb" }] },
    );
    expect(rows.map((r) => r.artifactId)).toEqual(["report", "scratch"]);
    expect(rows[0]!.mediaType).toBe("text/markdown");
    expect(rows[0]!.versions).toEqual([
      { version: 2, sha256: "bb", pinned: true },
      { version: 1, sha256: "aa", pinned: false },
    ]);
    expect(rows[1]!.visibility).toBe("internal");
  });
});

describe("hash router", () => {
  it("keeps page routes behind # so they never collide with API paths", () => {
    expect(tasksHref()).toBe("#/tasks");
    expect(taskHref("a/b?c")).toBe("#/tasks/a%2Fb%3Fc");
    expect(parseHash(taskHref("a/b?c"))).toEqual({ name: "task", id: "a/b?c" });
    expect(parseHash("#/tasks")).toEqual({ name: "tasks" });
    expect(parseHash("")).toEqual({ name: "tasks" });
    expect(parseHash("#/tasks/x/events")).toEqual({ name: "tasks" });
    for (const href of [tasksHref(), taskHref("t-1")]) expect(href.startsWith("#")).toBe(true);
  });
});

describe("describeError", () => {
  it("gives specific messages for diagnostic_mode, ownership_lost and budget errors", () => {
    const d = describeError(new ApiError(503, "diagnostic_mode", "server is in diagnostic mode"));
    expect(d.title).toContain("诊断模式");
    expect(d.detail).toContain("server is in diagnostic mode");
    expect(describeError(new ApiError(503, "ownership_lost", "")).title).toContain("所有权");
    expect(describeError(new ApiError(402, "budget_exhausted", "")).title).toContain("预算已耗尽");
    expect(describeError(new ApiError(400, "budget_something_new", "")).title).toBe("预算限制");
    expect(describeError(new ApiError(418, "teapot", "x")).title).toContain("418");
    expect(describeError(new NetworkError("refused")).code).toBe("network");
  });
});

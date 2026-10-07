import { describe, expect, it } from "vitest";
import { ApiError, NetworkError } from "../api/client";
import { ev, task } from "../components/testkit";
import { collectArtifacts } from "./artifacts";
import { controlAvailability } from "./controls";
import { describeError } from "./errors";
import { downloadFilename, previewKindForDownload, previewKindOf } from "./media";
import {
  homeHref,
  isAdminRoute,
  legacyHref,
  loginHref,
  needsSession,
  parseHash,
  registerHref,
  researchHref,
  sessionHref,
  taskHref,
  tasksHref,
} from "./router";
import { canCancel, countEvidence, deriveProgress, formatCreated, reportFilename, researchTitle, reportTitle, statusLabel, userErrorMessage } from "./research";
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
    expect(tasksHref()).toBe("#/admin/tasks");
    expect(taskHref("a/b?c")).toBe("#/admin/tasks/a%2Fb%3Fc");
    expect(parseHash(taskHref("a/b?c"))).toEqual({ name: "task", id: "a/b?c" });
    expect(parseHash("#/admin")).toEqual({ name: "tasks" });
    expect(parseHash("#/admin/tasks")).toEqual({ name: "tasks" });
    expect(parseHash("#/tasks/t-1")).toEqual({ name: "task", id: "t-1" }); // 旧链接仍进工作台
    expect(parseHash("#/admin/tasks/x/events")).toEqual({ name: "tasks" });
    for (const href of [tasksHref(), taskHref("t-1"), homeHref(), loginHref(), registerHref(), researchHref("r")]) {
      expect(href.startsWith("#")).toBe(true);
    }
  });

  it("parses #/, #/s/<id>, #/research and keeps #/admin", () => {
    expect(parseHash("")).toEqual({ name: "chat" });
    expect(parseHash("#/")).toEqual({ name: "chat" });
    expect(parseHash("#/nope")).toEqual({ name: "chat" });
    expect(homeHref()).toBe("#/");
    expect(sessionHref("s/1 2")).toBe("#/s/s%2F1%202");
    expect(parseHash(sessionHref("s/1 2"))).toEqual({ name: "chat", id: "s/1 2" });
    expect(parseHash("#/s/abc/")).toEqual({ name: "chat", id: "abc" });
    expect(parseHash("#/s/%E0%A4%A")).toEqual({ name: "chat" }); // 坏编码回到新对话
    expect(parseHash("#/s/")).toEqual({ name: "chat" });
    expect(legacyHref()).toBe("#/research");
    expect(parseHash("#/research")).toEqual({ name: "legacy" });
    expect(parseHash("#/research/")).toEqual({ name: "legacy" });
    expect(parseHash("#/admin")).toEqual({ name: "tasks" });
    expect(parseHash("#/admin/tasks/t-1")).toEqual({ name: "task", id: "t-1" });
    for (const r of ["#/", "#/s/x", "#/research", "#/research/x"]) expect(needsSession(parseHash(r))).toBe(true);
    expect(isAdminRoute(parseHash("#/s/x"))).toBe(false);
  });

  it("routes the user pages", () => {
    expect(parseHash("#/login")).toEqual({ name: "login" });
    expect(parseHash("#/register")).toEqual({ name: "register" });
    expect(researchHref("a/b")).toBe("#/research/a%2Fb");
    expect(parseHash(researchHref("a/b"))).toEqual({ name: "research", id: "a/b" });
    expect(needsSession(parseHash("#/"))).toBe(true);
    expect(needsSession(parseHash("#/research/x"))).toBe(true);
    expect(needsSession(parseHash("#/login"))).toBe(false);
    expect(isAdminRoute(parseHash("#/admin"))).toBe(true);
    expect(isAdminRoute(parseHash("#/research/x"))).toBe(false);
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

describe("research helpers", () => {
  const ck = (seq: number, step: string) => ev(seq, "checkpoint_committed", { payload: { step_id: step } });

  it("derives plan → 子任务 → 报告 stages from checkpoint step ids", () => {
    expect(deriveProgress([], "queued")).toEqual({ plan: "pending", tasks: "pending", report: "pending", subtasksDone: 0 });
    expect(deriveProgress([], "running").plan).toBe("active");
    const mid = deriveProgress([ck(1, "plan"), ck(2, "task-1"), ck(3, "task-1"), ck(4, "task-2")], "running");
    expect(mid).toEqual({ plan: "done", tasks: "active", report: "pending", subtasksDone: 2 });
    expect(deriveProgress([ck(1, "plan"), ck(2, "task-1")], "failed")).toMatchObject({ plan: "done", tasks: "stopped", report: "pending" });
    expect(deriveProgress([], "succeeded")).toMatchObject({ plan: "done", tasks: "done", report: "done" });
    // 任务状态尚未刷新时，以 task_terminal 事件为准
    const term = ev(5, "task_terminal", { payload: { task_status: "cancelled" } });
    expect(deriveProgress([ck(1, "plan"), term], "running").tasks).toBe("stopped");
  });

  it("counts evidence entries and reads the report title", () => {
    const md = "# 主题\n\n正文 [1]\n\n## 证据\n\n- [1] a — https://a — sha256:1\n- [2] b — https://b — sha256:2\n";
    expect(countEvidence(md)).toBe(2);
    expect(countEvidence("# t\n\n## 证据\n\n暂无证据。\n")).toBe(0);
    expect(countEvidence("no section")).toBe(0);
    expect(reportTitle(md)).toBe("主题");
    expect(reportFilename('a/b:c*?"<>|', "t-123456789")).toBe("a b c.md");
    expect(reportFilename("", "t-123456789")).toBe("研究报告-t-123456.md");
    const local = new Date(2026, 9, 6, 16, 5);
    expect(formatCreated(local.toISOString(), new Date(2026, 0, 1))).toBe("10月6日 16:05");
    expect(formatCreated(local.toISOString(), new Date(2027, 0, 1))).toBe("2026年10月6日 16:05");
    expect(formatCreated("bad")).toBe("");
    expect(researchTitle({ task_id: "t-123456789", topic: "  主题 " })).toBe("主题");
    expect(researchTitle({ task_id: "t-123456789" })).toBe("研究 t-123456");
  });

  it("maps errors to friendly copy without codes", () => {
    expect(userErrorMessage(new ApiError(409, "user_task_running", ""))).toBe("已有研究在进行中，请等它完成后再开始新的研究");
    expect(userErrorMessage(new ApiError(429, "rate_limited", ""))).toBe("尝试过于频繁，请稍后再试");
    expect(userErrorMessage(new ApiError(401, "invalid_credentials", ""))).toBe("用户名或密码错误");
    expect(userErrorMessage(new ApiError(409, "username_taken", ""))).toBe("用户名已被使用");
    expect(userErrorMessage(new ApiError(402, "budget_exhausted", "budget"))).not.toMatch(/budget|预算/);
    expect(userErrorMessage(new NetworkError("x"))).toContain("无法连接");
    expect(statusLabel("succeeded")).toBe("已完成");
    expect(canCancel(task({ status: "running", desired: "run" }))).toBe(true);
    expect(canCancel(task({ status: "running", desired: "cancel" }))).toBe(false);
    expect(canCancel(task({ status: "succeeded" }))).toBe(false);
  });
});

import { passwordChecks, passwordError, PASSWORD_RULE_TEXT } from "./password";

describe("password rule", () => {
  it("accepts 8–16 chars with two of digit/upper/lower", () => {
    for (const pw of ["abcdefG1", "ABCDEFG1", "abcdefgH", "Abcdefghijklmnop", "密码abcD1234"]) expect(passwordError(pw)).toBe("");
  });
  it("rejects length or class violations", () => {
    expect(passwordChecks("abcdeG1")).toEqual({ length: false, classes: true });
    expect(passwordChecks("abcdefghijklmnoP1")).toEqual({ length: false, classes: true });
    expect(passwordChecks("abcdefgh")).toEqual({ length: true, classes: false });
    expect(passwordError("密码密码密码密码")).toBe(PASSWORD_RULE_TEXT);
  });
});

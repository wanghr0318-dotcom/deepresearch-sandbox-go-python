import { mount } from "@vue/test-utils";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import App from "../App.vue";
import { ApiError, NetworkError } from "../api/client";
import type { User } from "../api/client";
import type { ChatApiLike } from "../api/chat";
import type { SessionApiLike } from "../api/session";
import { fakeChat, fakeSessionStream, session } from "../components/chat/chatkit";
import type { Mocked as ChatMocked } from "../components/chat/chatkit";
import { ev, fakeApi, fakeWatch, flushAll, task } from "../components/testkit";
import type { FakeWatch } from "../components/testkit";
import { servicesKey } from "../lib/services";
import type { Services } from "../lib/services";
import { userServicesKey } from "../lib/userServices";
import type { UserServices } from "../lib/userServices";
import AssistantView from "./AssistantView.vue";
import LoginView from "./LoginView.vue";
import RegisterView from "./RegisterView.vue";
import ResearchDetailView from "./ResearchDetailView.vue";

const ALICE: User = { username: "alice", role: "user" };

type Mocked<T> = { [K in keyof T]: T[K] & ReturnType<typeof vi.fn> };

function fakeSessionApi(over: Partial<SessionApiLike> = {}): Mocked<SessionApiLike> {
  const base: SessionApiLike = {
    register: vi.fn(async (username: string) => ({ username, role: "user" as const })),
    login: vi.fn(async (username: string) => ({ username, role: "user" as const })),
    logout: vi.fn(async () => {}),
    me: vi.fn(async () => ALICE),
    createResearch: vi.fn(async () => ({ task_id: "t-new" })),
    listTasks: vi.fn(async () => ({ tasks: [] })),
    getTask: vi.fn(async (id: string) => task({ task_id: id })),
    getResult: vi.fn(async () => ({ summary: "", outputs: [] })),
    cancelTask: vi.fn(async (id: string) => ({ task_id: id, control_version: 1 })),
    downloadArtifact: vi.fn(async () => ({ blob: new Blob([""]), contentType: "text/markdown", etag: "", contentDisposition: "" })),
  };
  for (const [k, fn] of Object.entries(over)) {
    (base as unknown as Record<string, unknown>)[k] = vi.isMockFunction(fn) ? fn : vi.fn(fn as (...a: unknown[]) => unknown);
  }
  return base as Mocked<SessionApiLike>;
}

interface Kit {
  api: Mocked<SessionApiLike>;
  chat: ChatMocked<ChatApiLike>;
  watch: FakeWatch;
  saveBlob: ReturnType<typeof vi.fn>;
  global: { provide: Record<symbol, unknown> };
}

function kit(over: Partial<SessionApiLike> = {}, admin?: Services, chatOver: Partial<ChatApiLike> = {}): Kit {
  const api = fakeSessionApi(over);
  const chat = fakeChat(chatOver);
  const watch = fakeWatch();
  const saveBlob = vi.fn();
  const us: UserServices = { api, chat, watch: watch.fn, watchSession: fakeSessionStream().fn, saveBlob };
  const provide: Record<symbol, unknown> = { [userServicesKey as symbol]: us };
  if (admin) provide[servicesKey as symbol] = admin;
  return { api, chat, watch, saveBlob, global: { provide } };
}

async function fill(w: ReturnType<typeof mount>, values: Record<string, string>): Promise<void> {
  for (const [name, v] of Object.entries(values)) await w.get(`[name="${name}"]`).setValue(v);
}

beforeEach(() => {
  window.location.hash = "";
});

afterEach(() => {
  window.location.hash = "";
});

describe("LoginView", () => {
  it("validates before sending and shows the binding error copy", async () => {
    const login = vi
      .fn()
      .mockRejectedValueOnce(new ApiError(401, "invalid_credentials", "invalid username or password"))
      .mockRejectedValueOnce(new ApiError(429, "rate_limited", "too many"))
      .mockResolvedValueOnce(ALICE);
    const k = kit({ login });
    const w = mount(LoginView, { global: k.global });
    await w.get("form").trigger("submit");
    expect(w.get('[role="alert"]').text()).toBe("请输入用户名和密码");
    expect(login).not.toHaveBeenCalled();

    await fill(w, { username: "alice", password: "wrong-password" });
    await w.get("form").trigger("submit");
    await flushAll();
    expect(w.get('[role="alert"]').text()).toBe("用户名或密码错误");
    expect(w.text()).not.toContain("invalid_credentials");

    await w.get("form").trigger("submit");
    await flushAll();
    expect(w.get('[role="alert"]').text()).toBe("尝试过于频繁，请稍后再试");

    await w.get("form").trigger("submit");
    await flushAll();
    expect(login).toHaveBeenLastCalledWith("alice", "wrong-password");
    expect(w.emitted("signed-in")).toEqual([[ALICE]]);
    expect(w.find('[role="alert"]').exists()).toBe(false);
  });
});

describe("RegisterView", () => {
  it("checks username, password and confirmation; maps username_taken and rate_limited", async () => {
    const register = vi
      .fn()
      .mockRejectedValueOnce(new ApiError(409, "username_taken", "username taken"))
      .mockRejectedValueOnce(new ApiError(429, "rate_limited", ""))
      .mockResolvedValueOnce(ALICE);
    const k = kit({ register });
    const w = mount(RegisterView, { global: k.global });

    await fill(w, { username: "a b", password: "Passw0rdX", confirm: "Passw0rdX" });
    await w.get("form").trigger("submit");
    expect(w.get('[role="alert"]').text()).toContain("用户名需为 3–32 位");

    await fill(w, { username: "alice", password: "abcdefgh", confirm: "abcdefgh" });
    await w.get("form").trigger("submit");
    expect(w.get('[role="alert"]').text()).toContain("两种");

    await fill(w, { password: "Passw0rdX", confirm: "Passw0rd9" });
    await w.get("form").trigger("submit");
    expect(w.get('[role="alert"]').text()).toBe("两次输入的密码不一致");
    expect(register).not.toHaveBeenCalled();

    await fill(w, { confirm: "Passw0rdX" });
    await w.get("form").trigger("submit");
    await flushAll();
    expect(w.get('[role="alert"]').text()).toBe("用户名已被使用");

    await w.get("form").trigger("submit");
    await flushAll();
    expect(w.get('[role="alert"]').text()).toBe("尝试过于频繁，请稍后再试");

    await w.get("form").trigger("submit");
    await flushAll();
    expect(register).toHaveBeenLastCalledWith("alice", "Passw0rdX");
    expect(w.emitted("signed-in")).toEqual([[ALICE]]);
  });

  it("shows live password checks and disables submit until both pass", async () => {
    const k = kit({ register: vi.fn() });
    const w = mount(RegisterView, { global: k.global });
    const check = (name: string) => w.get(`[data-testid="pw-checks"] [data-check="${name}"]`);
    const submit = () => w.get('button[type="submit"]');

    await fill(w, { username: "alice", password: "abc", confirm: "abc" });
    for (const name of ["length", "classes"]) {
      expect(check(name).text()).toContain("✗");
      expect(check(name).classes()).not.toContain("ok");
    }
    expect(submit().attributes("disabled")).toBeDefined();

    await fill(w, { password: "Passw0rdX", confirm: "Passw0rdX" });
    for (const name of ["length", "classes"]) {
      expect(check(name).text()).toContain("✓");
      expect(check(name).classes()).toContain("ok");
    }
    expect(submit().attributes("disabled")).toBeUndefined();
  });
});

describe("App routing and session guard", () => {
  it("redirects a signed-out visitor to #/login, then to the chat page after login", async () => {
    const me = vi.fn(async () => null);
    const k = kit({ me }, undefined, { listSessions: vi.fn(async () => ({ sessions: [session({ session_id: "s1", title: "固态电池" })] })) });
    const w = mount(App, { global: k.global });
    await flushAll();
    expect(me).toHaveBeenCalledTimes(1);
    expect(window.location.hash).toBe("#/login");
    expect(w.find(".chat-view").exists()).toBe(false);
    expect(w.text()).toContain("欢迎回来");

    await fill(w, { username: "alice", password: "password123" });
    await w.get("form").trigger("submit");
    await flushAll();
    expect(window.location.hash).toBe("#/");
    expect(w.find(".chat-view").exists()).toBe(true);
    expect(w.find(".assistant").exists()).toBe(false);
    expect(w.get('[data-testid="username"]').text()).toBe("alice");
    expect(w.get("[data-session=s1]").text()).toContain("固态电池");
    expect(w.get('[data-testid="nav-chat"]').classes()).toContain("on");
    w.unmount();
  });

  it("opens a session at #/s/<id> and keeps the early research list at #/research", async () => {
    window.location.hash = "#/s/s1";
    const k = kit(
      { listTasks: vi.fn(async () => ({ tasks: [task({ task_id: "t-1", status: "succeeded", topic: "钠离子电池" })] })) },
      undefined,
      { listTurns: vi.fn(async () => ({ turns: [] })) },
    );
    const w = mount(App, { global: k.global });
    await flushAll();
    expect(k.chat.listTurns).toHaveBeenCalledWith("s1");
    expect(w.find(".chat-view").exists()).toBe(true);
    expect(w.get('[data-testid="nav-legacy"]').attributes("href")).toBe("#/research");

    window.location.hash = "#/research";
    await flushAll();
    expect(w.find(".chat-view").exists()).toBe(false);
    expect(w.find(".assistant").exists()).toBe(true);
    expect(w.get('[data-testid="nav-legacy"]').classes()).toContain("on");
    expect(w.get('li[data-task="t-1"]').text()).toContain("钠离子电池");
    expect(w.get('li[data-task="t-1"] .badge').text()).toBe("已完成");
    expect(w.get('li[data-task="t-1"] [data-testid="created"]').text()).toMatch(/^\d+月\d+日 \d\d:\d\d$/);
    w.unmount();
  });

  it("also guards #/research/<id>, #/research and #/s/<id>, and sends a signed-in user away from #/login", async () => {
    for (const hash of ["#/research/t-1", "#/research", "#/s/x"]) {
      window.location.hash = hash;
      const k = kit({ me: vi.fn(async () => null) });
      const w = mount(App, { global: k.global });
      await flushAll();
      expect(window.location.hash).toBe("#/login");
      expect(k.watch.fn).not.toHaveBeenCalled();
      expect(k.chat.listTurns).not.toHaveBeenCalled();
      expect(k.api.listTasks).not.toHaveBeenCalled();
      w.unmount();
    }

    window.location.hash = "#/login";
    const k2 = kit();
    const w2 = mount(App, { global: k2.global });
    await flushAll();
    expect(window.location.hash).toBe("#/");
    w2.unmount();
  });

  it("logs out through the API and returns to #/login", async () => {
    const k = kit();
    const w = mount(App, { global: k.global });
    await flushAll();
    await w.get('[data-action="logout"]').trigger("click");
    await flushAll();
    expect(k.api.logout).toHaveBeenCalledTimes(1);
    expect(window.location.hash).toBe("#/login");
    w.unmount();
  });

  it("returns to #/login when the session expires mid-use (401) on the chat page and the early research page", async () => {
    const k = kit({}, undefined, { listSessions: vi.fn().mockRejectedValue(new ApiError(401, "unauthorized", "")) });
    const w = mount(App, { global: k.global });
    await flushAll();
    expect(window.location.hash).toBe("#/login");
    w.unmount();

    window.location.hash = "#/research";
    const k2 = kit({ listTasks: vi.fn().mockRejectedValue(new ApiError(401, "unauthorized", "")) });
    const w2 = mount(App, { global: k2.global });
    await flushAll();
    expect(window.location.hash).toBe("#/login");
    w2.unmount();
  });

  it("keeps the operator workbench at #/admin behind the TokenGate, without touching the user session", async () => {
    window.location.hash = "#/admin";
    const admin = fakeApi();
    const k = kit({}, { api: admin, watch: fakeWatch().fn, saveBlob: vi.fn() });
    const w = mount(App, { global: k.global });
    await flushAll();
    expect(w.text()).toContain("连接 agentbox");
    expect(w.find('input[aria-label="访问令牌"]').exists()).toBe(true);
    expect(w.find(".assistant").exists()).toBe(false);
    expect(w.find(".chat-view").exists()).toBe(false);
    expect(k.chat.listSessions).not.toHaveBeenCalled();
    expect(k.api.me).not.toHaveBeenCalled();
    expect(admin.listTasks).not.toHaveBeenCalled();
    expect(window.location.hash).toBe("#/admin");
    w.unmount();
  });
});

describe("AssistantView", () => {
  it("submits a topic, remembers it, and opens the research page", async () => {
    const k = kit();
    const w = mount(AssistantView, { global: k.global });
    await flushAll();
    expect(w.text()).toContain("还没有研究");
    await w.get('textarea[name="topic"]').setValue("  固态电池的商业化进展  ");
    await w.get("form").trigger("submit");
    await flushAll();
    expect(k.api.createResearch).toHaveBeenCalledTimes(1);
    expect(k.api.createResearch.mock.calls[0]![0]).toBe("固态电池的商业化进展");
    expect(typeof k.api.createResearch.mock.calls[0]![1]).toBe("string");
    expect(window.location.hash).toBe("#/research/t-new");
    w.unmount();
  });

  it("shows the user_task_running copy, and validates an empty topic locally", async () => {
    const createResearch = vi.fn().mockRejectedValue(new ApiError(409, "user_task_running", "x"));
    const k = kit({ createResearch, listTasks: vi.fn(async () => ({ tasks: [task({ task_id: "t-run", status: "running" })] })) });
    const w = mount(AssistantView, { global: k.global });
    await flushAll();
    expect(w.get(".running-note a").attributes("href")).toBe("#/research/t-run");
    expect(w.get('li[data-task="t-run"] .badge').text()).toBe("研究中");
    expect(w.get('li[data-task="t-run"] .research-topic').text()).toBe("研究 t-run"); // 服务端没有 topic 时的回退

    await w.get("form").trigger("submit");
    expect(w.get('[data-testid="submit-error"]').text()).toContain("主题");
    expect(createResearch).not.toHaveBeenCalled();

    await w.get('textarea[name="topic"]').setValue("新的主题");
    await w.get("form").trigger("submit");
    await flushAll();
    expect(w.get('[data-testid="submit-error"]').text()).toBe("已有研究在进行中，请等它完成后再开始新的研究");
    expect(window.location.hash).toBe("");
    w.unmount();
  });

  it("reuses the request_id after a network failure with the same topic", async () => {
    const createResearch = vi.fn().mockRejectedValueOnce(new NetworkError("down")).mockResolvedValueOnce({ task_id: "t-2" });
    const k = kit({ createResearch });
    const w = mount(AssistantView, { global: k.global });
    await w.get('textarea[name="topic"]').setValue("主题");
    await w.get("form").trigger("submit");
    await flushAll();
    expect(w.get('[data-testid="submit-error"]').text()).toContain("无法连接");
    await w.get("form").trigger("submit");
    await flushAll();
    expect(createResearch.mock.calls[1]![1]).toBe(createResearch.mock.calls[0]![1]);
    w.unmount();
  });
});

const HOSTILE_REPORT = [
  "# 固态电池",
  "",
  "正文第一段[1]。<script>window.__pwned = 1</script>",
  "",
  '<img src="x" onerror="window.__pwned = 2">',
  "",
  "[点我](javascript:alert(1))",
  "",
  "## 证据",
  "",
  "- [1] 来源一 — https://a.example — sha256:aa",
  "- [2] 来源二 — https://b.example — sha256:bb",
  "- [3] 来源三 — https://c.example — sha256:cc",
  "",
].join("\n");

describe("ResearchDetailView", () => {
  it("subscribes to events and shows plan → 子任务 → 报告 progress", async () => {
    const k = kit({ getTask: vi.fn(async () => task({ task_id: "t-1", status: "running", topic: "固态电池" })) });
    const w = mount(ResearchDetailView, { props: { id: "t-1" }, global: k.global });
    await flushAll();
    expect(k.watch.opts().taskId).toBe("t-1");
    expect(k.watch.opts().cursor).toBe(0);
    expect(w.get('[data-testid="topic"]').text()).toBe("固态电池");
    expect(w.get('[data-stage="plan"]').attributes("data-state")).toBe("active");
    expect(w.get(".badge").text()).toBe("研究中");

    k.watch.emit(ev(1, "task_created"));
    k.watch.emit(ev(2, "checkpoint_committed", { payload: { step_id: "plan", commit_seq: 1, checkpoint_id: "c1" } }));
    k.watch.emit(ev(3, "checkpoint_committed", { payload: { step_id: "task-1", commit_seq: 2, checkpoint_id: "c2" } }));
    k.watch.emit(ev(4, "checkpoint_committed", { payload: { step_id: "task-2", commit_seq: 3, checkpoint_id: "c3" } }));
    await flushAll();
    expect(w.get('[data-stage="plan"]').attributes("data-state")).toBe("done");
    expect(w.get('[data-stage="tasks"]').attributes("data-state")).toBe("active");
    expect(w.get('[data-stage="tasks"]').text()).toContain("已完成 2 个子任务");
    expect(w.get('[data-stage="report"]').attributes("data-state")).toBe("pending");
    expect(w.get('[data-testid="subtasks"]').text()).toBe("2");
    expect(w.get('[data-testid="evidence"]').text()).toBe("—");
    w.unmount();
    expect(k.watch.closed()).toBe(true);
  });

  it("renders the finished report through DOMPurify, counts evidence and downloads it", async () => {
    const getTask = vi.fn().mockResolvedValueOnce(task({ status: "running" })).mockResolvedValue(task({ status: "succeeded" }));
    const getResult = vi.fn(async () => ({ summary: "s", outputs: [{ artifact_id: "report", version: 2, sha256: "a".repeat(64) }] }));
    const blob = new Blob([HOSTILE_REPORT], { type: "text/markdown" });
    const downloadArtifact = vi.fn(async () => ({ blob, contentType: "text/markdown; charset=utf-8", etag: "", contentDisposition: "" }));
    const k = kit({ getTask, getResult, downloadArtifact });
    const w = mount(ResearchDetailView, { props: { id: "t-1" }, global: k.global, attachTo: document.body });
    await flushAll();
    k.watch.emit(ev(5, "checkpoint_committed", { payload: { step_id: "report" } }));
    k.watch.emit(ev(6, "task_terminal", { payload: { task_status: "succeeded", status_reason: "" } }));
    await flushAll();

    expect(getResult).toHaveBeenCalledWith("t-1");
    expect(downloadArtifact).toHaveBeenCalledWith("t-1", "report", 2);
    const report = w.get('[data-testid="report"]');
    expect(report.find("h1").text()).toBe("固态电池");
    const html = report.html();
    expect(html).not.toContain("<script");
    expect(html).not.toContain("onerror");
    expect(html).not.toContain("javascript:");
    expect((window as unknown as Record<string, unknown>).__pwned).toBeUndefined();
    for (const stage of ["plan", "tasks", "report"]) expect(w.get(`[data-stage="${stage}"]`).attributes("data-state")).toBe("done");
    expect(w.get('[data-testid="evidence"]').text()).toBe("3");
    expect(w.get('[data-testid="topic"]').text()).toBe("固态电池"); // 来自报告标题
    expect(w.find('[data-action="cancel"]').exists()).toBe(false);
    expect(w.get(".badge").text()).toBe("已完成");

    await w.get('[data-action="download"]').trigger("click");
    expect(k.saveBlob).toHaveBeenCalledWith(blob, "固态电池.md");
    w.unmount();
  });

  it("never shows cost, model, budget or call details", async () => {
    const k = kit({ getTask: vi.fn(async () => task({ status: "running", current_attempt_id: "att-secret" })) });
    const w = mount(ResearchDetailView, { props: { id: "t-1" }, global: k.global });
    await flushAll();
    k.watch.emit(ev(2, "checkpoint_committed", { payload: { step_id: "plan", checkpoint_id: "ck-internal" } }));
    await flushAll();
    const text = w.text();
    for (const s of ["费用", "成本", "模型", "预算", "调用", "token", "model", "cost", "att-secret", "ck-internal", "attempt"]) {
      expect(text).not.toContain(s);
    }
    w.unmount();
  });

  it("lets the user cancel (only cancel, no pause/resume)", async () => {
    const getTask = vi.fn().mockResolvedValueOnce(task({ status: "running" })).mockResolvedValue(task({ status: "cancelling", desired: "cancel" }));
    const k = kit({ getTask });
    const w = mount(ResearchDetailView, { props: { id: "t-1" }, global: k.global });
    await flushAll();
    expect(w.find('[data-action="pause"]').exists()).toBe(false);
    expect(w.find('[data-action="resume"]').exists()).toBe(false);
    await w.get('[data-action="cancel"]').trigger("click");
    await flushAll();
    expect(k.api.cancelTask).toHaveBeenCalledWith("t-1");
    expect(w.find('[data-action="cancel"]').exists()).toBe(false);
    expect(w.get(".badge").text()).toBe("正在取消");
    w.unmount();
  });

  it("shows a friendly page for someone else's or a missing task", async () => {
    const k = kit({ getTask: vi.fn().mockRejectedValue(new ApiError(404, "task_not_found", "task not found")) });
    const w = mount(ResearchDetailView, { props: { id: "t-x" }, global: k.global });
    await flushAll();
    expect(w.text()).toContain("找不到这项研究");
    // 返回早期研究列表（#/research），而不是对话页
    for (const a of w.findAll("a")) expect(a.attributes("href")).toBe("#/research");
    expect(k.watch.closed()).toBe(true);
    w.unmount();
  });

  it("explains a cancelled research without a report", async () => {
    const k = kit({
      getTask: vi.fn(async () => task({ status: "cancelled", desired: "cancel" })),
      getResult: vi.fn().mockRejectedValue(new ApiError(409, "not_ready", "")),
    });
    const w = mount(ResearchDetailView, { props: { id: "t-1" }, global: k.global });
    await flushAll();
    expect(w.text()).toContain("这项研究已取消");
    expect(w.get('[data-stage="plan"]').attributes("data-state")).toBe("stopped");
    w.unmount();
  });
});

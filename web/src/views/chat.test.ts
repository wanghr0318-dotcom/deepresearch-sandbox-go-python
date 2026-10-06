import { flushPromises, mount } from "@vue/test-utils";
import type { VueWrapper } from "@vue/test-utils";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ApiError } from "../api/client";
import type { ChatApi } from "../api/chat";
import { fakeChat, fakeSessionStream, session, sev, turn } from "../components/chat/chatkit";
import type { FakeSessionStream, Mocked } from "../components/chat/chatkit";
import type { ChatApiLike } from "../api/chat";
import Composer from "../components/chat/Composer.vue";
import MessageBubble from "../components/chat/MessageBubble.vue";
import SessionSidebar from "../components/chat/SessionSidebar.vue";
import { userServicesKey } from "../lib/userServices";
import type { UserServices } from "../lib/userServices";
import ChatView from "./ChatView.vue";

interface Deps {
  chat?: Mocked<ChatApiLike>;
  watchSession?: ChatApi["watchSession"];
}

function mountChat(deps: Deps, props: { sessionId?: string }): VueWrapper {
  const chat = deps.chat ?? fakeChat();
  const us = {
    api: {} as UserServices["api"],
    chat,
    watch: vi.fn() as unknown as UserServices["watch"],
    watchSession: deps.watchSession ?? fakeSessionStream().fn,
    saveBlob: vi.fn(),
  } satisfies UserServices;
  return mount(ChatView, { props, global: { provide: { [userServicesKey as symbol]: us } }, attachTo: document.body });
}

async function type(w: VueWrapper, text: string): Promise<void> {
  await w.get("textarea[name=message]").setValue(text);
}

async function send(w: VueWrapper): Promise<void> {
  await w.get("form.composer").trigger("submit");
  await flushPromises();
}

let wrappers: VueWrapper[] = [];
function track(w: VueWrapper): VueWrapper {
  wrappers.push(w);
  return w;
}

beforeEach(() => {
  window.location.hash = "";
});

afterEach(() => {
  for (const w of wrappers) w.unmount();
  wrappers = [];
  window.location.hash = "";
  vi.unstubAllGlobals();
});

describe("SessionSidebar", () => {
  const list = [
    session({ session_id: "a", title: "旧对话", last_active_at: "2026-10-01T08:00:00Z" }),
    session({ session_id: "b", title: "", last_active_at: "2026-10-06T08:00:00Z" }),
    session({ session_id: "c", title: "中间", last_active_at: "2026-10-03T08:00:00Z" }),
  ];

  it("lists sessions newest first, untitled as 新对话, and marks the active one", async () => {
    const w = track(mount(SessionSidebar, { props: { sessions: list, activeId: "c", loading: false } }));
    const items = w.findAll("[data-session]");
    expect(items.map((i) => i.attributes("data-session"))).toEqual(["b", "c", "a"]);
    expect(items[0]!.text()).toContain("新对话");
    expect(w.get("[data-session=c]").classes()).toContain("on");
    expect(w.get("[data-session=c] [data-action=select]").attributes("aria-current")).toBe("page");
    await w.get("[data-action=new-chat]").trigger("click");
    await w.get("[data-session=a] [data-action=select]").trigger("click");
    expect(w.emitted("create")).toHaveLength(1);
    expect(w.emitted("select")).toEqual([["a"]]);
  });

  it("renames inline: Enter saves, Esc cancels, invalid titles are rejected locally", async () => {
    const w = track(mount(SessionSidebar, { props: { sessions: list, loading: false }, attachTo: document.body }));
    await w.get("[data-session=a] [data-action=menu]").trigger("click");
    await w.get("[data-session=a] [data-action=rename]").trigger("click");
    const input = w.get("[data-session=a] input[name=title]");
    expect((input.element as HTMLInputElement).value).toBe("旧对话");
    await input.setValue("   ");
    await input.trigger("keydown", { key: "Enter" });
    expect(w.get("[data-session=a] [role=alert]").text()).toBe("标题不能为空");
    expect(w.emitted("rename")).toBeUndefined();
    await input.setValue("  新标题 ");
    await input.trigger("keydown", { key: "Enter" });
    expect(w.emitted("rename")).toEqual([["a", "新标题"]]);
    expect(w.find("[data-session=a] input[name=title]").exists()).toBe(false);

    await w.get("[data-session=c] [data-action=menu]").trigger("click");
    await w.get("[data-session=c] [data-action=rename]").trigger("click");
    await w.get("[data-session=c] input[name=title]").setValue("不要");
    await w.get("[data-session=c] input[name=title]").trigger("keydown", { key: "Escape" });
    expect(w.find("[data-session=c] input[name=title]").exists()).toBe(false);
    expect(w.emitted("rename")).toHaveLength(1);
  });

  it("asks before deleting", async () => {
    const w = track(mount(SessionSidebar, { props: { sessions: list, loading: false } }));
    await w.get("[data-session=a] [data-action=menu]").trigger("click");
    await w.get("[data-session=a] [data-action=delete]").trigger("click");
    expect(w.get("[data-session=a]").text()).toContain("删除这个对话？");
    await w.get("[data-session=a] [data-action=cancel-delete]").trigger("click");
    expect(w.emitted("remove")).toBeUndefined();
    await w.get("[data-session=a] [data-action=menu]").trigger("click");
    await w.get("[data-session=a] [data-action=delete]").trigger("click");
    await w.get("[data-session=a] [data-action=confirm-delete]").trigger("click");
    expect(w.emitted("remove")).toEqual([["a"]]);
  });

  it("shows loading and empty states", () => {
    const w = track(mount(SessionSidebar, { props: { sessions: [], loading: true } }));
    expect(w.text()).toContain("加载中…");
    const w2 = track(mount(SessionSidebar, { props: { sessions: [], loading: false } }));
    expect(w2.text()).toContain("还没有对话");
  });
});

describe("Composer", () => {
  it("sends on Enter with the toggle, keeps Shift+Enter and IME composition for typing", async () => {
    const w = track(mount(Composer, { props: { disabled: false } }));
    const ta = w.get("textarea[name=message]");
    const toggle = w.get("[data-action=deep-research]");
    expect(toggle.attributes("aria-pressed")).toBe("false");
    await toggle.trigger("click");
    expect(toggle.attributes("aria-pressed")).toBe("true");

    await ta.setValue("你好");
    await ta.trigger("keydown", { key: "Enter", shiftKey: true });
    await ta.trigger("keydown", { key: "Enter", isComposing: true });
    await ta.trigger("compositionstart");
    await ta.trigger("keydown", { key: "Enter" });
    await ta.trigger("compositionend");
    expect(w.emitted("send")).toBeUndefined();

    await ta.trigger("keydown", { key: "Enter" });
    expect(w.emitted("send")).toEqual([[{ text: "你好", deepResearch: true }]]);
  });

  it("validates locally and disables send while a turn is running", async () => {
    const w = track(mount(Composer, { props: { disabled: false } }));
    expect(w.get("[data-action=send]").attributes("disabled")).toBeDefined();
    await w.get("textarea[name=message]").setValue("长".repeat(4001));
    await w.get("form").trigger("submit");
    expect(w.get("[role=alert]").text()).toBe("消息不能超过 4000 个字符");
    expect(w.emitted("send")).toBeUndefined();

    await w.setProps({ running: true });
    await w.get("textarea[name=message]").setValue("再问一句");
    expect(w.get("[data-action=send]").attributes("disabled")).toBeDefined();
    expect(w.get("[data-testid=composer-hint]").text()).toBe("先停止当前研究");
    await w.get("form").trigger("submit");
    expect(w.emitted("send")).toBeUndefined();
    await w.get("[data-action=stop-turn]").trigger("click");
    expect(w.emitted("stop")).toHaveLength(1);
  });
});

describe("MessageBubble", () => {
  it("puts the user on the right and the assistant (with avatar) on the left", () => {
    const u = track(mount(MessageBubble, { props: { role: "user" }, slots: { default: "问" } }));
    const a = track(mount(MessageBubble, { props: { role: "assistant" }, slots: { default: "<p>答</p>" } }));
    expect(u.get("[data-role=user]").classes()).toContain("user");
    expect(u.text()).toBe("问");
    expect(a.get("[data-role=assistant]").classes()).toContain("assistant");
    expect(a.find(".avatar").exists()).toBe(true);
  });
});

describe("ChatView", () => {
  it("starts a new chat: first message creates a session, routes to #/s/<id>, and sends with the toggle", async () => {
    const chat = fakeChat({ createSession: vi.fn(async () => session({ session_id: "s9" })) });
    const stream = fakeSessionStream();
    const w = track(mountChat({ chat, watchSession: stream.fn }, {}));
    await flushPromises();
    expect(w.text()).toContain("有什么想了解的？");
    await type(w, "固态电池现状");
    await w.find("[data-action=deep-research]").trigger("click");
    await send(w);
    expect(chat.createSession).toHaveBeenCalled();
    expect(window.location.hash).toBe("#/s/s9");
    expect(chat.sendMessage.mock.calls[0]!.slice(0, 3)).toEqual(["s9", "固态电池现状", true]);
    // 发送成功后立即显示用户气泡，输入框清空
    expect(w.get("[data-role=user]").text()).toBe("固态电池现状");
    expect((w.get("textarea[name=message]").element as HTMLTextAreaElement).value).toBe("");
    expect(stream.opts()).toEqual(expect.objectContaining({ sessionId: "s9", cursor: 0 }));
  });

  it("renders user bubbles on the right and the AI turn on the left from replayed events", async () => {
    const stream = fakeSessionStream();
    const chat = fakeChat({ listTurns: vi.fn(async () => ({ turns: [turn({ turn_id: "u1", text: "问" })] })) });
    const w = track(mountChat({ chat, watchSession: stream.fn }, { sessionId: "s1" }));
    await flushPromises();
    stream.push(sev(1, "route", { route: "answer" }, "u1"));
    stream.push(sev(2, "assistant_delta", { text: "答", final: true }, "u1"));
    await flushPromises();
    expect(chat.listTurns).toHaveBeenCalledWith("s1");
    expect(w.get("[data-role=user]").text()).toBe("问");
    expect(w.get("[data-role=assistant]").text()).toContain("答");
    expect(stream.opts()).toEqual(expect.objectContaining({ sessionId: "s1", cursor: 0 }));
  });

  it("disables send while a turn is running (409 turn_in_progress is never provoked) and stops it", async () => {
    const stream = fakeSessionStream();
    const chat = fakeChat({ listTurns: vi.fn(async () => ({ turns: [turn({ turn_id: "u1", text: "研究", status: "running" })] })) });
    const w = track(mountChat({ chat, watchSession: stream.fn }, { sessionId: "s1" }));
    await flushPromises();
    await type(w, "再问");
    expect(w.get("[data-action=send]").attributes("disabled")).toBeDefined();
    expect(w.get("[data-testid=composer-hint]").text()).toBe("先停止当前研究");
    await send(w);
    expect(chat.sendMessage).not.toHaveBeenCalled();

    await w.get("[data-action=stop-turn]").trigger("click");
    await flushPromises();
    expect(chat.control).toHaveBeenCalledWith("u1", "stop");

    stream.push(sev(3, "turn_status", { status: "cancelled" }, "u1"));
    await flushPromises();
    expect(w.get("[data-action=send]").attributes("disabled")).toBeUndefined();
    await send(w);
    expect(chat.sendMessage.mock.calls[0]!.slice(0, 3)).toEqual(["s1", "再问", false]);
  });

  it("shows user copy for errors, reuses the request id on retry, and emits unauthorized on 401", async () => {
    const sendMessage = vi
      .fn()
      .mockRejectedValueOnce(new ApiError(503, "sessions_unavailable", "x"))
      .mockRejectedValueOnce(new ApiError(500, "weird_code", "x"))
      .mockRejectedValueOnce(new ApiError(401, "unauthorized", ""));
    const chat = fakeChat({ sendMessage });
    const w = track(mountChat({ chat }, { sessionId: "s1" }));
    await flushPromises();
    await type(w, "你好");
    await send(w);
    expect(w.get("[data-testid=chat-error]").text()).toBe("对话服务暂未开放，请稍后再试");
    expect(w.text()).not.toContain("sessions_unavailable");
    await send(w);
    expect(w.get("[data-testid=chat-error]").text()).toBe("服务暂时不可用，请稍后再试");
    expect(sendMessage.mock.calls[1]![3]).toBe(sendMessage.mock.calls[0]![3]);
    await send(w);
    expect(w.emitted("unauthorized")).toHaveLength(1);
  });

  it("sidebar renames and deletes sessions; deleting the active one returns to #/", async () => {
    const chat = fakeChat({
      listSessions: vi.fn(async () => ({
        sessions: [session({ session_id: "s1", title: "固态电池" }), session({ session_id: "s2", title: "钠电池", last_active_at: "2026-10-01T00:00:00Z" })],
      })),
    });
    const stream = fakeSessionStream();
    window.location.hash = "#/s/s1";
    const w = track(mountChat({ chat, watchSession: stream.fn }, { sessionId: "s1" }));
    await flushPromises();
    expect(w.get("[data-session=s1]").classes()).toContain("on");

    await w.get("[data-session=s2] [data-action=menu]").trigger("click");
    await w.get("[data-session=s2] [data-action=rename]").trigger("click");
    await w.get("[data-session=s2] input[name=title]").setValue("钠离子电池");
    await w.get("[data-session=s2] input[name=title]").trigger("keydown", { key: "Enter" });
    await flushPromises();
    expect(chat.renameSession).toHaveBeenCalledWith("s2", "钠离子电池");
    expect(w.get("[data-session=s2]").text()).toContain("钠离子电池");

    await w.get("[data-session=s1] [data-action=menu]").trigger("click");
    await w.get("[data-session=s1] [data-action=delete]").trigger("click");
    await w.get("[data-session=s1] [data-action=confirm-delete]").trigger("click");
    await flushPromises();
    expect(chat.deleteSession).toHaveBeenCalledWith("s1");
    expect(w.find("[data-session=s1]").exists()).toBe(false);
    expect(window.location.hash).toBe("#/");
    expect(stream.closed()).toBe(true);
    expect(w.text()).toContain("有什么想了解的？");
  });

  it("selects a session from the sidebar and starts a new chat with 新对话", async () => {
    const chat = fakeChat({ listSessions: vi.fn(async () => ({ sessions: [session({ session_id: "s2", title: "钠电池" })] })) });
    const stream = fakeSessionStream();
    const w = track(mountChat({ chat, watchSession: stream.fn }, {}));
    await flushPromises();
    await w.get("[data-session=s2] [data-action=select]").trigger("click");
    await flushPromises();
    expect(window.location.hash).toBe("#/s/s2");
    expect(chat.listTurns).toHaveBeenCalledWith("s2");
    expect(stream.opts().sessionId).toBe("s2");

    await w.get("[data-action=new-chat]").trigger("click");
    await flushPromises();
    expect(window.location.hash).toBe("#/");
    expect(stream.closed()).toBe(true);
  });

  it("question answer, stop, continue, finish and restore call the turn endpoints", async () => {
    const stream = fakeSessionStream();
    const chat = fakeChat({ listTurns: vi.fn(async () => ({ turns: [turn({ turn_id: "u1", text: "研究", deep_research: true, status: "running" })] })) });
    const w = track(mountChat({ chat, watchSession: stream.fn }, { sessionId: "s1" }));
    await flushPromises();
    stream.push(sev(1, "route", { route: "research", forced: true }, "u1"));
    stream.push(sev(2, "turn_status", { status: "running" }, "u1"));
    await flushPromises();

    // 停止（轮次头部的按钮）
    await w.get(".turn [data-action=stop]").trigger("click");
    await flushPromises();
    expect(chat.control).toHaveBeenLastCalledWith("u1", "stop");

    // 提问 → 回答
    stream.push(sev(3, "ask_user", { question_id: "q1", questions: [{ id: "q-a", question: "地区？", options: ["中国", "全球"], allow_other: false }] }, "u1"));
    stream.push(sev(4, "turn_status", { status: "awaiting_input" }, "u1"));
    await flushPromises();
    const opt = w.findAll("[data-action=option]").find((b) => b.text() === "中国");
    expect(opt).toBeDefined();
    await opt!.trigger("click");
    await w.get("[data-action=submit-answers]").trigger("click");
    await flushPromises();
    expect(chat.answer).toHaveBeenCalledWith("u1", [{ question_id: "q-a", choice: "中国" }]);

    // 暂停 → 继续 / 立即写报告
    stream.push(sev(5, "turn_stopped", { card: { subtopics_done: 1, subtopics_total: 3, sources: 4, tool_calls_used: 9, tool_call_limit: 30 }, findings: "f", can_finish: true }, "u1"));
    stream.push(sev(6, "turn_status", { status: "paused" }, "u1"));
    await flushPromises();
    await w.get("[data-action=continue]").trigger("click");
    await flushPromises();
    expect(chat.control).toHaveBeenLastCalledWith("u1", "continue");
    await w.get("[data-action=finish]").trigger("click");
    await flushPromises();
    expect(chat.control).toHaveBeenLastCalledWith("u1", "finish");

    // 已停止 → 恢复
    stream.push(sev(7, "turn_status", { status: "cancelled" }, "u1"));
    await flushPromises();
    await w.get("[data-action=restore]").trigger("click");
    await flushPromises();
    expect(chat.restore).toHaveBeenCalledWith("u1");
  });

  it("side panel follows the active research turn; tabs switch between 进度/来源/报告", async () => {
    const stream = fakeSessionStream();
    const chat = fakeChat({
      listTurns: vi.fn(async () => ({
        turns: [turn({ turn_id: "u1", turn_index: 1, text: "研究 A", deep_research: true, status: "succeeded" }), turn({ turn_id: "u2", turn_index: 2, text: "闲聊", status: "succeeded" })],
      })),
    });
    const w = track(mountChat({ chat, watchSession: stream.fn }, { sessionId: "s1" }));
    await flushPromises();
    stream.push(sev(1, "route", { route: "research" }, "u1"));
    stream.push(sev(2, "todo_updated", { items: [{ id: "t1", title: "梳理技术路线", status: "done" }] }, "u1"));
    stream.push(sev(3, "tool_result", { tool: "web_fetch", ok: true, preview: { kind: "fetch", url: "https://a.example/x", title: "来源甲", n: 1 } }, "u1"));
    stream.push(sev(4, "route", { route: "answer" }, "u2"));
    await flushPromises();
    const panel = w.get(".side-panel");
    expect(panel.text()).toContain("梳理技术路线");
    await panel.get("[data-tab=sources]").trigger("click");
    expect(w.get(".side-panel").text()).toContain("来源甲");
    await w.get(".side-panel [data-tab=report]").trigger("click");
    expect(w.get(".side-panel").text()).toContain("报告还没有生成");
    // 轮次里的"查看进度"切回进度标签
    await w.get("[data-action=view-progress]").trigger("click");
    expect(w.get(".side-panel [data-tab=progress]").attributes("aria-selected")).toBe("true");
  });

  it("shows 正在恢复对话… while the session is restoring", async () => {
    const stream = fakeSessionStream();
    const w = track(mountChat({ watchSession: stream.fn }, { sessionId: "s1" }));
    await flushPromises();
    expect(w.find("[data-testid=restoring]").exists()).toBe(false);
    stream.push(sev(1, "session_state", { state: "restoring" }));
    await flushPromises();
    expect(w.get("[data-testid=restoring]").text()).toBe("正在恢复对话…");
    stream.push(sev(2, "session_state", { state: "running" }));
    await flushPromises();
    expect(w.find("[data-testid=restoring]").exists()).toBe(false);
    stream.push(sev(3, "session_state", { state: "evicted", user_message: "会话暂时无法恢复" }));
    await flushPromises();
    expect(w.get("[data-testid=session-message]").text()).toBe("会话暂时无法恢复");
  });

  it("shows 正在恢复对话… when a message is sent to an evicted session", async () => {
    let release: () => void = () => {};
    const sendMessage = vi.fn(() => new Promise<{ turn_id: string; turn_index: number }>((r) => (release = () => r({ turn_id: "u9", turn_index: 1 }))));
    const chat = fakeChat({ listSessions: vi.fn(async () => ({ sessions: [session({ session_id: "s1", state: "evicted" })] })), sendMessage });
    const w = track(mountChat({ chat }, { sessionId: "s1" }));
    await flushPromises();
    expect(w.find("[data-testid=restoring]").exists()).toBe(false);
    await type(w, "还在吗");
    await send(w);
    expect(w.get("[data-testid=restoring]").text()).toBe("正在恢复对话…");
    release();
    await flushPromises();
    expect(w.find("[data-testid=restoring]").exists()).toBe(false);
  });

  it("collapses to a single column with drawers on narrow screens", async () => {
    vi.stubGlobal("matchMedia", (q: string) => ({ matches: q.includes("900px"), media: q, addEventListener: () => {}, removeEventListener: () => {} }));
    const chat = fakeChat({ listSessions: vi.fn(async () => ({ sessions: [session({ session_id: "s2", title: "钠电池" })] })) });
    const w = track(mountChat({ chat }, { sessionId: "s1" }));
    await flushPromises();
    expect(w.get(".chat-view").classes()).toContain("narrow");
    expect(w.find(".sidebar").exists()).toBe(false);
    expect(w.find(".side-panel").exists()).toBe(false);

    await w.get("[data-action=open-sessions]").trigger("click");
    expect(w.find(".drawer .sidebar").exists()).toBe(true);
    await w.get("[data-action=close-drawer]").trigger("click");
    expect(w.find(".sidebar").exists()).toBe(false);

    await w.get("[data-action=open-panel]").trigger("click");
    expect(w.find(".drawer .side-panel").exists()).toBe(true);
    await w.get(".drawer").trigger("keydown", { key: "Escape" });
    expect(w.find(".side-panel").exists()).toBe(false);
  });

  it("switching sessions closes the previous event stream", async () => {
    const streams: FakeSessionStream[] = [fakeSessionStream(), fakeSessionStream()];
    let n = 0;
    const watchSession = vi.fn((opts: Parameters<ChatApi["watchSession"]>[0]) => streams[n++]!.fn(opts)) as unknown as ChatApi["watchSession"];
    const chat = fakeChat({ listTurns: vi.fn(async (id: string) => ({ turns: [turn({ turn_id: `u-${id}`, text: `问 ${id}` })] })) });
    const w = track(mountChat({ chat, watchSession }, { sessionId: "s1" }));
    await flushPromises();
    expect(w.get("[data-role=user]").text()).toBe("问 s1");
    await w.setProps({ sessionId: "s2" });
    await flushPromises();
    expect(streams[0]!.closed()).toBe(true);
    expect(streams[1]!.opts().sessionId).toBe("s2");
    expect(w.findAll("[data-role=user]").map((b) => b.text())).toEqual(["问 s2"]);
    w.unmount();
    wrappers = [];
    expect(streams[1]!.closed()).toBe(true);
  });

  it("emits unauthorized when loading sessions returns 401", async () => {
    const chat = fakeChat({ listSessions: vi.fn().mockRejectedValue(new ApiError(401, "unauthorized", "")) });
    const w = track(mountChat({ chat }, {}));
    await flushPromises();
    expect(w.emitted("unauthorized")).toHaveLength(1);
  });

  it("shows a friendly note for a missing session and does not subscribe", async () => {
    const stream = fakeSessionStream();
    const chat = fakeChat({ listTurns: vi.fn().mockRejectedValue(new ApiError(404, "session_not_found", "")) });
    const w = track(mountChat({ chat, watchSession: stream.fn }, { sessionId: "gone" }));
    await flushPromises();
    expect(w.text()).toContain("找不到这个对话，可能已被删除");
    expect(stream.fn).not.toHaveBeenCalled();
  });
});

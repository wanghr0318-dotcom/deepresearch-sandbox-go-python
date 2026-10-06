import { describe, expect, it } from "vitest";
import { sev, turn } from "../components/chat/chatkit";
import { ApiError } from "../api/client";
import { USER_ERRORS, activeResearchTurn, applyEvent, chatErrorMessage, emptyChat, recordAnswers, seedTurns, siteOf } from "./chat";
import type { ChatState } from "./chat";
import type { SessionEvent } from "../api/chat";

const SHA = "c".repeat(64);
const r = (title: string, url: string) => ({ title, url, snippet: `${title} 摘要` });

function run(state: ChatState, events: SessionEvent[]): ChatState {
  return events.reduce(applyEvent, state);
}

function started(id = "u1", over: Parameters<typeof turn>[0] = {}): ChatState {
  return seedTurns(emptyChat(), [turn({ turn_id: id, text: "固态电池", status: "running", deep_research: true, ...over })]);
}

describe("siteOf", () => {
  it("returns the host without www. and an empty string for junk", () => {
    expect(siteOf("https://www.a.example/x?y")).toBe("a.example");
    expect(siteOf("http://B.example:8080/")).toBe("b.example");
    expect(siteOf("not a url")).toBe("");
  });
});

describe("applyEvent", () => {
  it("builds a search step that expands to one line per result and counts them in the title", () => {
    let s = started();
    s = applyEvent(s, sev(1, "tool_call", { step_id: "orch", tool_call_id: "c1", tool: "web_search", input: { query: "固态电池" } }, "u1"));
    expect(s.turns[0]!.steps[0]).toMatchObject({ id: "c1", kind: "search", status: "running", detail: "固态电池" });
    s = applyEvent(
      s,
      sev(2, "tool_result", {
        step_id: "orch",
        tool_call_id: "c1",
        tool: "web_search",
        ok: true,
        preview: { kind: "search", query: "固态电池", results: [r("A", "https://www.a.example/x"), r("B", "https://b.example/y")] },
        raw: { request: { query: "固态电池" }, response_ref: SHA },
      }, "u1"),
    );
    const row = s.turns[0]!.steps[0]!;
    expect(s.turns[0]!.steps).toHaveLength(1);
    expect(row.status).toBe("done");
    expect(row.title).toBe("搜索网页（2 条结果）");
    expect(row.results?.map((x) => x.site)).toEqual(["a.example", "b.example"]);
    expect(row.results?.[0]).toEqual({ title: "A", url: "https://www.a.example/x", site: "a.example", snippet: "A 摘要" });
    expect(row.raw).toEqual({ request: { query: "固态电池" }, requestTruncated: false, responseRef: SHA });
    expect(s.cursor).toBe(2);
    expect(applyEvent(s, sev(2, "tool_result", {}, "u1"))).toBe(s); // 重放的旧 seq 忽略
    expect(applyEvent(s, sev(1, "tool_call", {}, "u1"))).toBe(s);
  });

  it("is pure: never mutates the previous state", () => {
    const s0 = started();
    const snapshot = JSON.stringify(s0);
    const s1 = applyEvent(s0, sev(1, "tool_call", { step_id: "orch", tool_call_id: "c1", tool: "web_fetch", input: { url: "https://x.example/a" } }, "u1"));
    applyEvent(s1, sev(2, "tool_result", { step_id: "orch", tool_call_id: "c1", tool: "web_fetch", ok: true, preview: { n: 1, title: "T", url: "https://x.example/a", excerpt: "e" } }, "u1"));
    expect(JSON.stringify(s0)).toBe(snapshot);
    expect(s1.turns[0]!.steps[0]!.status).toBe("running");
  });

  it("advances the cursor past unknown and internal events without changing turns", () => {
    const s0 = started();
    const s1 = applyEvent(s0, sev(4, "something_new", { x: 1 }, "u1"));
    expect(s1.cursor).toBe(4);
    expect(s1.turns).toBe(s0.turns);
    expect(applyEvent(s1, sev(5, "internal", {})).turns).toBe(s0.turns);
  });

  it("shows fetch, local tools and failures; only Gateway calls get ⟨/⟩", () => {
    const s = run(started(), [
      sev(1, "tool_call", { step_id: "orch", tool_call_id: "f1", tool: "web_fetch", input: { url: "https://www.news.example/p" } }, "u1"),
      sev(2, "tool_result", {
        step_id: "orch", tool_call_id: "f1", tool: "web_fetch", ok: true,
        preview: { kind: "fetch", n: 1, title: "新闻", url: "https://www.news.example/p", site: "news.example", excerpt: "摘录" },
        raw: { request: "{\"url\":", request_truncated: true, response_ref: SHA },
      }, "u1"),
      sev(3, "tool_call", { step_id: "orch", tool_call_id: "f2", tool: "web_fetch", input: { url: "https://down.example/" } }, "u1"),
      sev(4, "tool_result", { step_id: "orch", tool_call_id: "f2", tool: "web_fetch", ok: false, preview: {}, error: "网页无法访问" }, "u1"),
      sev(5, "tool_call", { step_id: "orch", tool_call_id: "k1", tool: "read_skill", input: { name: "deep-research" } }, "u1"),
      sev(6, "tool_result", { step_id: "orch", tool_call_id: "k1", tool: "read_skill", ok: true, preview: { kind: "text", text: "skill 正文" }, raw: { request: { name: "deep-research" } } }, "u1"),
      sev(7, "skill_read", { step_id: "orch", name: "deep-research", description: "深度研究" }, "u1"),
      sev(8, "tool_call", { step_id: "orch", tool_call_id: "x1", tool: "run_python", input: {} }, "u1"),
    ]);
    const [fetch, failed, skill, other] = s.turns[0]!.steps;
    expect(fetch).toMatchObject({ kind: "fetch", title: "阅读网页 · news.example", status: "done", text: "摘录" });
    expect(fetch!.raw).toEqual({ request: "{\"url\":", requestTruncated: true, responseRef: SHA });
    expect(failed).toMatchObject({ kind: "fetch", title: "阅读网页 · down.example", status: "error", text: "网页无法访问" });
    // read_skill 的工具行与随后的 skill_read 合为一行；本地工具没有 ⟨/⟩
    expect(skill).toMatchObject({ kind: "skill", title: "读取 skill：deep-research", status: "done", text: "skill 正文" });
    expect(skill!.raw).toBeUndefined();
    expect(other).toMatchObject({ kind: "tool", title: "调用工具：run_python", status: "running" });
    expect(s.turns[0]!.steps).toHaveLength(4);
    expect(s.turns[0]!.sources).toEqual([{ n: 1, title: "新闻", url: "https://www.news.example/p", site: "news.example" }]);
  });

  it("gives skill_read without a preceding tool call its own row, and thinking a row of its own", () => {
    const s = run(started(), [
      sev(1, "skill_read", { step_id: "orch", name: "deep-research", file: "report.md" }, "u1"),
      sev(2, "thinking", { step_id: "orch", text: "先拆分子主题", raw: { request: { messages: [] }, response_ref: SHA } }, "u1"),
      sev(3, "thinking", { step_id: "orch", text: "再想想" }, "u1"),
    ]);
    expect(s.turns[0]!.steps.map((x) => [x.id, x.kind, x.title, x.status])).toEqual([
      ["skill_read-1", "skill", "读取 skill：deep-research · report.md", "done"],
      ["thinking-2", "thinking", "思考", "done"],
      ["thinking-3", "thinking", "思考", "done"],
    ]);
    expect(s.turns[0]!.steps[1]!.raw?.responseRef).toBe(SHA);
    expect(s.turns[0]!.steps[2]!.raw).toBeUndefined();
  });

  it("tracks todo, subtopics, budget, sources and report", () => {
    let s = run(started(), [
      sev(1, "route", { route: "research", forced: true }, "u1"),
      sev(2, "todo_updated", { items: [{ id: "1", title: "技术路线", status: "pending", budget_share: 10 }, { id: "2", title: "产业化", status: "pending" }] }, "u1"),
      sev(3, "tool_call", { step_id: "orch", tool_call_id: "s1", tool: "research_subtopic", input: { id: "1" } }, "u1"),
      sev(4, "subtopic", { id: "1", title: "技术路线", status: "running" }, "u1"),
      sev(5, "tool_call", { step_id: "sub-1", tool_call_id: "c9", tool: "web_search", input: { query: "q" }, subtopic_id: "1" }, "u1"),
      sev(6, "budget", { used: 1, limit: 30 }, "u1"),
    ]);
    const t = s.turns[0]!;
    expect(t.route).toBe("research");
    expect(t.routeForced).toBe(true);
    expect(t.todo).toEqual([
      { id: "1", title: "技术路线", status: "pending", budgetShare: 10 },
      { id: "2", title: "产业化", status: "pending" },
    ]);
    expect(t.steps[0]).toMatchObject({ kind: "subtopic", title: "研究子主题：技术路线", status: "running", subtopicId: "1" });
    expect(t.steps[1]).toMatchObject({ kind: "search", subtopicId: "1", subtopic: "技术路线" });
    expect(t.subtopics).toEqual([{ id: "1", title: "技术路线", status: "running" }]);
    expect(t.budget).toEqual({ used: 1, limit: 30 });

    s = run(s, [
      sev(7, "subtopic", { id: "1", title: "技术路线", status: "done", summary: "硫化物路线领先 [1]" }, "u1"),
      sev(8, "report_ready", { artifact_id: "report", version: 1, title: "固态电池研究", partial: false, tool_budget_reached: true, note: "已达工具额度（30/30）" }, "u1"),
    ]);
    const t2 = s.turns[0]!;
    expect(t2.subtopics[0]).toEqual({ id: "1", title: "技术路线", status: "done", summary: "硫化物路线领先 [1]" });
    expect(t2.steps[0]).toMatchObject({ status: "done", text: "硫化物路线领先 [1]" });
    expect(t2.report).toEqual({ artifactId: "report", version: 1, title: "固态电池研究", partial: false, toolBudgetReached: true, note: "已达工具额度（30/30）" });
    expect(activeResearchTurn(s)?.turnId).toBe("u1");
  });

  it("asks the user, waits, and marks the question answered when the turn resumes", () => {
    let s = run(started(), [
      sev(1, "tool_call", { step_id: "orch", tool_call_id: "a1", tool: "ask_user", input: {} }, "u1"),
      sev(2, "ask_user", {
        step_id: "ask",
        question_id: "q-1",
        questions: [
          { id: "q-1-0", question: "时间范围？", options: ["近一年", "近五年"], allow_other: true },
          { id: "q-1-1", question: "地区？", options: ["中国", "全球"], allow_other: false },
        ],
      }, "u1"),
      sev(3, "turn_status", { status: "awaiting_input" }, "u1"),
    ]);
    let t = s.turns[0]!;
    expect(t.status).toBe("awaiting_input");
    expect(t.steps).toHaveLength(1);
    expect(t.steps[0]).toMatchObject({ kind: "ask", title: "向你提问", status: "done" });
    expect(t.question).toEqual({
      questionId: "q-1",
      questions: [
        { id: "q-1-0", question: "时间范围？", options: ["近一年", "近五年"], allowOther: true },
        { id: "q-1-1", question: "地区？", options: ["中国", "全球"], allowOther: false },
      ],
      answered: false,
    });
    s = recordAnswers(s, "u1", ["近一年", "中国"]);
    expect(s.turns[0]!.question?.answers).toEqual(["近一年", "中国"]);
    s = applyEvent(s, sev(4, "turn_status", { status: "running" }, "u1"));
    t = s.turns[0]!;
    expect(t.question?.answered).toBe(true);
    expect(t.question?.answers).toEqual(["近一年", "中国"]);
  });

  it("does not mark a superseded question as answered", () => {
    const s = run(started(), [
      sev(1, "ask_user", { step_id: "ask", question_id: "q", questions: [{ id: "a", question: "?", options: ["x", "y"], allow_other: true }] }, "u1"),
      sev(2, "turn_status", { status: "awaiting_input" }, "u1"),
      sev(3, "turn_status", { status: "cancelled", reason: "superseded" }, "u1"),
    ]);
    expect(s.turns[0]!.question?.answered).toBe(false);
    expect(s.turns[0]!.canRestore).toBe(true);
  });

  it("shows the stop card, clears it on continue and settles running steps", () => {
    let s = run(started(), [
      sev(1, "tool_call", { step_id: "orch", tool_call_id: "c1", tool: "web_search", input: { query: "q" } }, "u1"),
      sev(2, "turn_status", { status: "stopping" }, "u1"),
      sev(3, "turn_status", { status: "paused", reason: "user" }, "u1"),
      sev(4, "turn_stopped", {
        card: {
          subtopics_done: 1, subtopics_total: 3, sources: 4, tool_calls_used: 7, tool_call_limit: 30,
          todo: [{ id: "1", title: "A", status: "done" }, { id: "2", title: "B", status: "in_progress" }],
        },
        findings: "目前发现……",
        can_finish: true,
      }, "u1"),
    ]);
    let t = s.turns[0]!;
    expect(t.status).toBe("paused");
    expect(t.steps[0]!.status).toBe("done");
    expect(t.stop).toEqual({
      todo: [{ id: "1", title: "A", status: "done" }, { id: "2", title: "B", status: "in_progress" }],
      subtopicsDone: 1,
      subtopicsTotal: 3,
      sources: 4,
      budget: { used: 7, limit: 30 },
      findings: "目前发现……",
      canFinish: true,
    });
    expect(t.todo.map((x) => x.status)).toEqual(["done", "in_progress"]);
    expect(t.canRestore).toBe(false);
    s = applyEvent(s, sev(5, "turn_status", { status: "running" }, "u1"));
    t = s.turns[0]!;
    expect(t.stop).toBeUndefined();
    expect(t.status).toBe("running");
  });

  it("applies a superseded cancel that arrives after the next turn started, and links a restore", () => {
    let s = run(started("u1"), [
      sev(1, "turn_stopped", { card: { subtopics_done: 0, subtopics_total: 2, sources: 0, tool_calls_used: 2, tool_call_limit: 30 }, findings: "尚无发现，研究在规划阶段被停止", can_finish: false }, "u1"),
      sev(2, "turn_status", { status: "paused" }, "u1"),
      sev(3, "turn_created", { turn_index: 2, text: "换个问题", deep_research: false }, "u2"),
      sev(4, "turn_status", { status: "queued" }, "u2"),
      sev(5, "turn_status", { status: "running" }, "u2"),
      sev(6, "assistant_delta", { message_id: "m1", text: "好的" }, "u2"),
      sev(7, "turn_status", { status: "cancelled", reason: "superseded" }, "u1"),
    ]);
    expect(s.turns.map((t) => [t.turnId, t.status])).toEqual([["u1", "cancelled"], ["u2", "running"]]);
    expect(s.turns[0]!.canRestore).toBe(true);
    expect(s.turns[0]!.stop?.findings).toBe("尚无发现，研究在规划阶段被停止");
    expect(s.turns[1]!.reply).toBe("好的");
    s = run(s, [
      sev(8, "turn_status", { status: "succeeded" }, "u2"),
      sev(9, "turn_created", { turn_index: 3, text: "固态电池", deep_research: true, restored_from_turn_id: "u1" }, "u3"),
    ]);
    expect(s.turns.map((t) => t.turnId)).toEqual(["u1", "u2", "u3"]);
    expect(s.turns[2]!.restoredFrom).toBe("u1");
    expect(s.turns[0]!.canRestore).toBe(false); // 已被恢复，不再显示"恢复"
    expect(activeResearchTurn(s)?.turnId).toBe("u3");
  });

  it("accumulates assistant_delta per message and marks the route", () => {
    let s = started("u1", { deep_research: false, text: "1+1？" });
    s = run(s, [
      sev(1, "route", { route: "answer", forced: false }, "u1"),
      sev(2, "assistant_delta", { message_id: "m1", text: "等于" }, "u1"),
      sev(3, "assistant_delta", { message_id: "m1", text: " 2。", final: true }, "u1"),
      sev(4, "assistant_delta", { message_id: "m2", text: "还有问题吗？", final: true }, "u1"),
      sev(5, "turn_result", { summary: "ignored because deltas arrived", outputs: [] }, "u1"),
      sev(6, "turn_status", { status: "succeeded" }, "u1"),
    ]);
    const t = s.turns[0]!;
    expect(t.route).toBe("answer");
    expect(t.reply).toBe("等于 2。\n\n还有问题吗？");
    expect(t.status).toBe("succeeded");
    expect(activeResearchTurn(s)).toBeUndefined();
  });

  it("falls back to turn_result.summary when no delta was seen (resumed late)", () => {
    const s = applyEvent(started(), sev(9, "turn_result", { summary: "完整回复", outputs: ["report"] }, "u1"));
    expect(s.turns[0]!.reply).toBe("完整回复");
  });

  it("maps a failed turn to the user-facing copy and never exposes the raw code", () => {
    let s = applyEvent(started(), sev(1, "turn_status", { status: "failed", reason: "model_unavailable" }, "u1"));
    expect(s.turns[0]!.error).toBe("模型服务暂时不可用，请重试");
    expect(USER_ERRORS.model_unavailable).toBe("模型服务暂时不可用，请重试");
    s = applyEvent(started(), sev(1, "turn_status", { status: "failed", reason: "worker_crashed_xyz" }, "u1"));
    expect(s.turns[0]!.error).toBe("本轮未能完成，请重试");
    expect(s.turns[0]!.error).not.toContain("worker_crashed_xyz");
    s = applyEvent(started(), sev(1, "turn_status", { status: "failed", reason: "x", user_message: "模型服务暂时不可用，请重试" }, "u1"));
    expect(s.turns[0]!.error).toBe("模型服务暂时不可用，请重试");
    // 会话仍可继续：下一轮正常
    s = run(s, [sev(2, "turn_created", { turn_index: 2, text: "再试", deep_research: false }, "u2"), sev(3, "turn_status", { status: "running" }, "u2")]);
    expect(s.turns[1]!.error).toBeUndefined();
  });

  it("session_state restoring/evicted/idle updates sessionStatus", () => {
    let s = applyEvent(emptyChat(), sev(1, "session_state", { state: "restoring" }));
    expect(s.sessionStatus).toBe("restoring");
    s = applyEvent(s, sev(2, "session_state", { state: "evicted", user_message: "会话暂时无法恢复" }));
    expect(s).toMatchObject({ sessionStatus: "evicted", sessionMessage: "会话暂时无法恢复" });
    s = applyEvent(s, sev(3, "session_state", { state: "idle" }));
    expect(s.sessionStatus).toBe("idle");
    expect(s.sessionMessage).toBeUndefined();
  });

  it("creates a placeholder turn for events of a turn it has not seen", () => {
    const s = applyEvent(emptyChat(), sev(3, "budget", { used: 2, limit: 30 }, "uX"));
    expect(s.turns).toHaveLength(1);
    expect(s.turns[0]).toMatchObject({ turnId: "uX", userText: "", budget: { used: 2, limit: 30 } });
  });
});

describe("seedTurns", () => {
  it("builds views from history and keeps live state when re-seeded", () => {
    const history = [
      turn({ turn_id: "u2", turn_index: 2, text: "B", status: "succeeded", route: "answer", summary: "回答 B" }),
      turn({
        turn_id: "u1", turn_index: 1, text: "A", deep_research: true, status: "cancelled", status_reason: "superseded",
        route: "research", restorable: true, tool_calls_used: 12, report: { artifact_id: "report", version: 1 },
      }),
      turn({ turn_id: "u3", turn_index: 3, text: "C", status: "failed", user_message: "模型服务暂时不可用，请重试" }),
    ];
    let s = seedTurns(emptyChat(), history);
    expect(s.turns.map((t) => t.turnId)).toEqual(["u1", "u2", "u3"]);
    expect(s.turns[0]).toMatchObject({
      userText: "A", deepResearch: true, status: "cancelled", statusReason: "superseded", route: "research", canRestore: true,
      budget: { used: 12, limit: 30 },
      report: { artifactId: "report", version: 1, title: "研究报告", partial: false, toolBudgetReached: false },
    });
    expect(s.turns[1]!.reply).toBe("回答 B");
    expect(s.turns[2]!.error).toBe("模型服务暂时不可用，请重试");
    s = applyEvent(s, sev(10, "assistant_delta", { message_id: "m", text: "x" }, "u3"));
    const again = seedTurns(s, [turn({ turn_id: "u3", turn_index: 3, text: "C", status: "running" })]);
    expect(again.turns[2]!.reply).toBe("x");
    expect(again.turns[2]!.status).toBe("running");
    expect(again.cursor).toBe(10);
    const restored = seedTurns(s, [turn({ turn_id: "u4", turn_index: 4, text: "A", deep_research: true, restored_from_turn_id: "u1" })]);
    expect(restored.turns[0]!.canRestore).toBe(false);
  });
});

describe("chatErrorMessage", () => {
  it("maps session error codes to user copy", () => {
    expect(chatErrorMessage(new ApiError(409, "turn_in_progress", ""))).toBe(USER_ERRORS.turn_in_progress);
    expect(USER_ERRORS.turn_in_progress).toContain("先停止当前研究");
    expect(chatErrorMessage(new ApiError(503, "session_unavailable", ""))).toBe("会话暂时无法恢复");
    expect(chatErrorMessage(new ApiError(500, "boom", ""))).toBe("服务暂时不可用，请稍后再试");
  });
});

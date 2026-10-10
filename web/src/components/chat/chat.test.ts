import { flushPromises, mount } from "@vue/test-utils";
import type { Component } from "vue";
import { describe, expect, it, vi } from "vitest";
import type { ChatApiLike } from "../../api/chat";
import type { Question, RawRef, StepRow as StepRowData, StopInfo, TurnView as TurnViewData } from "../../lib/chat";
import { applyEvent, emptyChat } from "../../lib/chat";
import { userServicesKey } from "../../lib/userServices";
import type { UserServices } from "../../lib/userServices";
import { fakeChat, fakeSessionStream, sev } from "./chatkit";
import QuestionCard from "./QuestionCard.vue";
import RawDialog from "./RawDialog.vue";
import SidePanel from "./SidePanel.vue";
import StepRow from "./StepRow.vue";
import StopCard from "./StopCard.vue";
import TurnView from "./TurnView.vue";

const SHA = "9f2c7d0b5e1a4c3f8a6b2d9e0c1f7a3b5d8e2c4f6a1b9d0e3c5f7a2b4d6e8f01";

function raw(over: Partial<RawRef> = {}): RawRef {
  return { request: { query: "固态电池" }, requestTruncated: false, responseRef: SHA, ...over };
}

function searchRow(n: number, over: Partial<StepRowData> = {}): StepRowData {
  const names = ["A", "B", "C", "D", "E", "F", "G", "H"];
  return {
    id: "root/orch/search/1",
    kind: "search",
    title: `搜索网页（${n} 条结果）`,
    detail: "固态电池 量产",
    status: "done",
    results: names.slice(0, n).map((x) => ({
      title: `标题${x}`,
      site: `${x.toLowerCase()}.example`,
      snippet: `摘要${x}`,
      url: `https://${x.toLowerCase()}.example/p`,
    })),
    raw: raw(),
    ...over,
  };
}

function svc(over: Partial<UserServices> = {}): UserServices {
  return {
    api: {} as UserServices["api"],
    chat: fakeChat(),
    watch: vi.fn() as unknown as UserServices["watch"],
    watchSession: fakeSessionStream().fn,
    saveBlob: vi.fn(),
    ...over,
  };
}

function mountWith<P extends Record<string, unknown>>(c: Component, props: P, services: Partial<UserServices> = {}) {
  const s = svc(services);
  const w = mount(c as never, { props: props as never, global: { provide: { [userServicesKey as symbol]: s } } });
  return Object.assign(w, { services: s });
}

function blankTurn(over: Partial<TurnViewData> = {}): TurnViewData {
  return {
    turnId: "u1",
    index: 1,
    userText: "问题",
    deepResearch: false,
    status: "running",
    steps: [],
    reply: "",
    todo: [],
    subtopics: [],
    sources: [],
    canRestore: false,
    ...over,
  };
}

function question(over: Partial<Question> = {}): Question {
  return {
    questionId: "q-1",
    questions: [
      { id: "region", question: "关注哪个地区？", options: ["中国", "全球"], allowOther: true },
      { id: "focus", question: "报告侧重？", options: ["技术路线", "产业化与成本"], allowOther: false },
    ],
    answered: false,
    ...over,
  };
}

function stopInfo(over: Partial<StopInfo> = {}): StopInfo {
  return {
    todo: [
      { id: "t1", title: "梳理技术路线", status: "done" },
      { id: "t2", title: "中国量产进度", status: "in_progress" },
      { id: "t3", title: "成本与供应链", status: "pending" },
    ],
    subtopicsDone: 1,
    subtopicsTotal: 3,
    sources: 4,
    budget: { used: 12, limit: 30 },
    findings: "硫化物路线进展最快。",
    canFinish: true,
    ...over,
  };
}

describe("StepRow", () => {
  it("collapses by default, expands search results line by line, and opens raw", async () => {
    const w = mount(StepRow, { props: { row: searchRow(3), expanded: false } });
    expect(w.text()).toContain("搜索网页「固态电池 量产」· 3 条结果");
    const head = w.get("[data-testid=step-head]");
    expect(head.attributes("aria-expanded")).toBe("false");
    expect(w.findAll("[data-testid=search-result]")).toHaveLength(0);

    await head.trigger("click");
    expect(w.emitted("toggle")).toHaveLength(1);

    await w.setProps({ expanded: true });
    expect(w.get("[data-testid=step-head]").attributes("aria-expanded")).toBe("true");
    const lines = w.findAll("[data-testid=search-result]");
    expect(lines.map((x) => x.text())).toEqual([
      expect.stringContaining("标题A"),
      expect.stringContaining("标题B"),
      expect.stringContaining("标题C"),
    ]);
    expect(lines[0]!.text()).toContain("a.example");
    expect(lines[0]!.text()).toContain("摘要A");
    const link = lines[0]!.get("a");
    expect(link.attributes("href")).toBe("https://a.example/p");
    expect(link.attributes("rel")).toBe("noopener noreferrer");
    expect(link.attributes("target")).toBe("_blank");

    await w.setProps({ expanded: false });
    expect(w.findAll("[data-testid=search-result]")).toHaveLength(0);

    await w.get("[aria-label=查看原始请求与响应]").trigger("click");
    expect(w.emitted("raw")?.[0]).toEqual([raw()]);
    expect(w.emitted("toggle")).toHaveLength(1); // ⟨/⟩ 不触发折叠
  });

  it("long result lists show the first five, then 还有 N 条 reveals the rest", async () => {
    const w = mount(StepRow, { props: { row: searchRow(8), expanded: true } });
    expect(w.findAll("[data-testid=search-result]")).toHaveLength(5);
    await w.get("[data-action=more-results]").trigger("click");
    expect(w.findAll("[data-testid=search-result]")).toHaveLength(8);
  });

  it("hides ⟨/⟩ without a raw ref and never renders internal ids", async () => {
    const row = searchRow(2, { raw: undefined, subtopicId: "sub-internal-7" });
    const w = mount(StepRow, { props: { row, expanded: true } });
    expect(w.find("[aria-label=查看原始请求与响应]").exists()).toBe(false);
    expect(w.html()).not.toContain("root/orch/search/1");
    expect(w.html()).not.toContain("sub-internal-7");

    const w2 = mount(StepRow, { props: { row: searchRow(2), expanded: true } });
    expect(w2.html()).not.toContain(SHA);
  });

  it("running rows show progress; thinking and fetch rows expand to their text", async () => {
    const running = mount(StepRow, {
      props: { row: { id: "c2", kind: "search", title: "搜索网页", detail: "硫化物 成本", status: "running" }, expanded: true },
    });
    expect(running.text()).toContain("搜索网页「硫化物 成本」…");
    expect(running.get("[data-testid=step-head]").attributes("data-status")).toBe("running");

    const fetch = mount(StepRow, {
      props: {
        row: { id: "c3", kind: "fetch", title: "阅读网页 · 36kr.com", detail: "https://36kr.com/p/1", status: "done", text: "<b>正文</b>摘录" },
        expanded: false,
      },
    });
    expect(fetch.text()).toContain("阅读网页 · 36kr.com");
    expect(fetch.text()).not.toContain("摘录");
    await fetch.setProps({ expanded: true });
    expect(fetch.text()).toContain("<b>正文</b>摘录");
    expect(fetch.find("b").exists()).toBe(false);

    const err = mount(StepRow, { props: { row: { id: "c4", kind: "fetch", title: "阅读网页", status: "error", text: "网页无法访问" }, expanded: true } });
    expect(err.get("[data-testid=step-head]").attributes("data-status")).toBe("error");
    expect(err.text()).toContain("网页无法访问");
  });

  it("运行代码 rows collapse, show exit code and the first 12 output lines, then reveal the rest", async () => {
    const lines = Array.from({ length: 20 }, (_, i) => `<i>line ${i + 1}</i>`);
    const row: StepRowData = {
      id: "orch:1",
      kind: "code",
      title: "运行代码",
      detail: "print(rows)",
      status: "done",
      text: ["退出码 0", ...lines].join("\n"),
      raw: raw({ request: { language: "python3", code: "print(rows)" } }),
    };
    const w = mount(StepRow, { props: { row, expanded: false } });
    expect(w.get("[data-testid=step-head]").text()).toContain("运行代码");
    expect(w.find("[data-testid=code-output]").exists()).toBe(false);
    await w.setProps({ expanded: true });
    const out = w.get("[data-testid=code-output]");
    expect(out.element.tagName).toBe("PRE");
    expect(out.text().split("\n")).toEqual(["退出码 0", ...lines.slice(0, 11)]);
    expect(w.find("i").exists()).toBe(false); // 输出按纯文本插值
    expect(w.text()).toContain("print(rows)");
    expect(w.get("[data-action=more-lines]").text()).toContain("还有 9 行");
    await w.get("[data-action=more-lines]").trigger("click");
    expect(w.get("[data-testid=code-output]").text().split("\n")).toHaveLength(21);
    expect(w.find("[data-action=more-lines]").exists()).toBe(false);
    await w.get("button.raw").trigger("click");
    expect(w.emitted("raw")![0]).toEqual([row.raw]);

    const short = mount(StepRow, { props: { row: { ...row, id: "orch:2", text: "退出码 1\nstderr：\nboom", status: "error" }, expanded: true } });
    expect(short.find("[data-action=more-lines]").exists()).toBe(false);
    expect(short.get("[data-testid=code-output]").text()).toBe("退出码 1\nstderr：\nboom");
    expect(short.text()).toContain("失败");
  });

  it("运行命令 (workspace) rows render like 运行代码; file and external-tool rows show plain text", async () => {
    const shell: StepRowData = {
      id: "orch:1", kind: "shell", title: "运行命令", detail: "python3 -m unittest -v", status: "done",
      text: "退出码 0\nRan 1 test\nOK", raw: raw({ request: { command: "python3 -m unittest -v" } }),
    };
    const w = mount(StepRow, { props: { row: shell, expanded: true } });
    expect(w.get("[data-testid=step-head]").text()).toContain("运行命令");
    expect(w.get("[data-testid=code-output]").text()).toBe("退出码 0\nRan 1 test\nOK");
    expect(w.get(".detail").classes()).toContain("mono");
    expect(w.find("button.raw").exists()).toBe(true);

    const file = mount(StepRow, { props: { row: { id: "orch:2", kind: "file", title: "写入文件 · calc.py", status: "done", text: "已新建 calc.py" }, expanded: true } });
    expect(file.find("[data-testid=code-output]").exists()).toBe(false);
    expect(file.find("button.raw").exists()).toBe(false);
    expect(file.text()).toContain("已新建 calc.py");

    const mcp = mount(StepRow, { props: { row: { id: "orch:3", kind: "mcp", title: "外部工具 · calc/calculate", status: "done", text: "6*7 = 42" }, expanded: false } });
    expect(mcp.get("[data-testid=step-head]").text()).toContain("外部工具 · calc/calculate");
  });
});

describe("RawDialog", () => {
  it("shows request/response as text and never renders HTML or the response ref", async () => {
    const getRaw = vi.fn(async () => ({ contentType: "application/json", body: { html: "<script>alert(2)</script>", ok: true } }));
    const ref = raw({ request: { q: "<img src=x onerror=alert(1)>" } });
    const w = mountWith(RawDialog, { turnId: "u1", rawRef: ref }, { chat: fakeChat({ getRaw }) });
    expect(w.text()).toContain("加载中");
    await flushPromises();
    expect(getRaw).toHaveBeenCalledWith("u1", SHA);
    expect(w.find("img").exists()).toBe(false);
    expect(w.find("script").exists()).toBe(false);
    expect(w.text()).toContain("<img src=x onerror=alert(1)>");
    expect(w.text()).toContain("<script>alert(2)</script>");
    expect(w.html()).not.toContain(SHA);
    expect(w.get("[role=dialog]").attributes("aria-modal")).toBe("true");

    await w.get("[data-action=close]").trigger("click");
    expect(w.emitted("close")).toHaveLength(1);
  });

  it("marks truncated requests, shows plain-text bodies and a failure message", async () => {
    const getRaw = vi.fn(async () => {
      throw new Error("boom");
    });
    const w = mountWith(RawDialog, { turnId: "u1", rawRef: raw({ request: "很长的请求…", requestTruncated: true }) }, { chat: fakeChat({ getRaw }) });
    await flushPromises();
    expect(w.text()).toContain("很长的请求…");
    expect(w.text()).toContain("已截断");
    expect(w.text()).toContain("原始响应加载失败");

    const w2 = mountWith(RawDialog, { turnId: "u1", rawRef: null });
    expect(w2.find("[role=dialog]").exists()).toBe(false);
  });

  it("closes on Escape", async () => {
    const w = mountWith(RawDialog, { turnId: "u1", rawRef: raw() });
    await flushPromises();
    await w.get("[role=dialog]").trigger("keydown", { key: "Escape" });
    expect(w.emitted("close")).toHaveLength(1);
  });
});

describe("QuestionCard", () => {
  it("requires every question, supports 其他…, then shows 已回答", async () => {
    const w = mount(QuestionCard, { props: { question: question(), disabled: false } });
    expect(w.text()).toContain("需要确认 2 个问题");
    const submit = w.get("[data-action=submit-answers]");
    expect(submit.attributes("disabled")).toBeDefined();

    // 第二题没有"其他…"
    const groups = w.findAll("[data-testid=question]");
    expect(groups[0]!.find("[data-action=other]").exists()).toBe(true);
    expect(groups[1]!.find("[data-action=other]").exists()).toBe(false);

    await groups[0]!.findAll("[data-action=option]")[0]!.trigger("click");
    expect(groups[0]!.findAll("[data-action=option]")[0]!.attributes("aria-pressed")).toBe("true");
    expect(w.get("[data-action=submit-answers]").attributes("disabled")).toBeDefined();

    // 改选"其他…"：空输入不算作答，超过 200 字也不算
    await groups[0]!.get("[data-action=other]").trigger("click");
    const input = groups[0]!.get("input");
    expect(input.attributes("maxlength")).toBe("200");
    await groups[1]!.findAll("[data-action=option]")[1]!.trigger("click");
    expect(w.get("[data-action=submit-answers]").attributes("disabled")).toBeDefined();
    await input.setValue("  东南亚  ");
    expect(w.get("[data-action=submit-answers]").attributes("disabled")).toBeUndefined();

    await w.get("[data-action=submit-answers]").trigger("click");
    expect(w.emitted("answer")?.[0]).toEqual([
      [
        { question_id: "region", other: "东南亚" },
        { question_id: "focus", choice: "产业化与成本" },
      ],
    ]);

    await w.setProps({ question: question({ answered: true, answers: ["东南亚", "产业化与成本"] }) });
    expect(w.text()).toContain("已回答");
    expect(w.text()).toContain("东南亚");
    expect(w.find("[data-action=submit-answers]").exists()).toBe(false);
    for (const b of w.findAll("[data-action=option]")) expect(b.attributes("disabled")).toBeDefined();
  });

  it("disabled card cannot submit", async () => {
    const q = question({ questions: [{ id: "x", question: "选一个", options: ["是", "否"], allowOther: false }] });
    const w = mount(QuestionCard, { props: { question: q, disabled: true } });
    await w.get("[data-action=option]").trigger("click");
    expect(w.get("[data-action=submit-answers]").attributes("disabled")).toBeDefined();
    await w.setProps({ disabled: false });
    await w.get("[data-action=option]").trigger("click");
    expect(w.get("[data-action=submit-answers]").attributes("disabled")).toBeUndefined();
  });
});

describe("StopCard", () => {
  it("continue always; finish disabled until a subtopic is done; restore for cancelled", async () => {
    const w = mount(StopCard, { props: { stop: stopInfo(), status: "paused", canRestore: false, busy: false } });
    expect(w.text()).toContain("研究已暂停");
    expect(w.text()).toContain("梳理技术路线");
    expect(w.text()).toContain("1 / 3");
    expect(w.text()).toContain("4");
    expect(w.text()).toContain("12 / 30");
    expect(w.text()).toContain("目前发现");
    expect(w.text()).toContain("硫化物路线进展最快。");
    await w.get("[data-action=continue]").trigger("click");
    await w.get("[data-action=finish]").trigger("click");
    expect(w.emitted("continue")).toHaveLength(1);
    expect(w.emitted("finish")).toHaveLength(1);

    await w.setProps({ stop: stopInfo({ canFinish: false, subtopicsDone: 0 }) });
    expect(w.get("[data-action=finish]").attributes("disabled")).toBeDefined();
    expect(w.get("[data-action=continue]").attributes("disabled")).toBeUndefined();
    expect(w.text()).toContain("至少完成一个子主题后可用");

    await w.setProps({ busy: true });
    expect(w.get("[data-action=continue]").attributes("disabled")).toBeDefined();

    const c = mount(StopCard, { props: { stop: stopInfo(), status: "cancelled", canRestore: true, busy: false } });
    expect(c.text()).toContain("已停止");
    expect(c.find("[data-action=continue]").exists()).toBe(false);
    await c.get("[data-action=restore]").trigger("click");
    expect(c.emitted("restore")).toHaveLength(1);

    const none = mount(StopCard, { props: { status: "cancelled", canRestore: false, busy: false } });
    expect(none.find("[data-action=restore]").exists()).toBe(false);
    const running = mount(StopCard, { props: { status: "running", canRestore: false, busy: false } });
    expect(running.text()).toBe("");
  });
});

describe("TurnView", () => {
  it("emits stop for a running turn and shows the route badge and failure copy", async () => {
    const w = mount(TurnView, { props: { turn: blankTurn({ route: "research", status: "running" }), busy: false } });
    expect(w.get("[data-testid=route]").text()).toBe("深度研究");
    await w.get("[data-action=stop]").trigger("click");
    expect(w.emitted("action")?.[0]).toEqual(["u1", "stop"]);

    await w.setProps({ turn: blankTurn({ route: "answer", status: "failed", error: "模型服务暂时不可用，请重试" }) });
    expect(w.get("[data-testid=route]").text()).toBe("直接回答");
    expect(w.find("[data-action=stop]").exists()).toBe(false);
    expect(w.get("[role=alert]").text()).toContain("模型服务暂时不可用，请重试");
  });

  it("labels a restored turn that has no route event (older worker) as a restored research", () => {
    const w = mount(TurnView, { props: { turn: blankTurn({ restoredFrom: "u0", status: "running" }), busy: false } });
    expect(w.get("[data-testid=route]").text()).toBe("已恢复的研究");
    expect(w.find("[data-action=view-progress]").exists()).toBe(true);
    const plain = mount(TurnView, { props: { turn: blankTurn({ status: "running" }), busy: false } });
    expect(plain.find("[data-testid=route]").exists()).toBe(false);
  });

  it("running rows start expanded, others collapsed; clicking toggles; subtopic headings group rows", async () => {
    const steps: StepRowData[] = [
      { id: "a", kind: "skill", title: "读取 skill：deep-research", status: "done", text: "说明" },
      searchRow(2, { id: "b", subtopic: "技术路线" }),
      { id: "c", kind: "fetch", title: "阅读网页 · a.example", status: "done", subtopic: "技术路线", text: "正文" },
      { id: "d", kind: "search", title: "搜索网页", detail: "成本", status: "running", subtopic: "成本与供应链" },
    ];
    const w = mount(TurnView, { props: { turn: blankTurn({ steps, route: "research" }), busy: false } });
    const heads = () => w.findAll("[data-testid=step-head]");
    expect(heads().map((h) => h.attributes("aria-expanded"))).toEqual(["false", "false", "false", "true"]);
    expect(w.findAll("[data-testid=subtopic-heading]").map((h) => h.text())).toEqual(["技术路线", "成本与供应链"]);

    await heads()[1]!.trigger("click");
    expect(w.findAll("[data-testid=search-result]")).toHaveLength(2);
    await heads()[1]!.trigger("click");
    expect(w.findAll("[data-testid=search-result]")).toHaveLength(0);
    await heads()[3]!.trigger("click");
    expect(heads()[3]!.attributes("aria-expanded")).toBe("false");
  });

  it("opens the raw dialog from a row and streams the reply as plain text", async () => {
    const turn = blankTurn({ steps: [searchRow(1)], reply: "第一段\n<b>不是 HTML</b>", status: "succeeded", route: "answer" });
    const w = mountWith(TurnView, { turn, busy: false });
    expect(w.get("[data-testid=reply]").text()).toContain("<b>不是 HTML</b>");
    expect(w.get("[data-testid=reply]").find("b").exists()).toBe(false);
    // 已结束的轮次：步骤链先折叠为"研究过程"，点开后才显示各行
    await w.get("[data-testid=chain-toggle]").trigger("click");
    await w.get("[aria-label=查看原始请求与响应]").trigger("click");
    await flushPromises();
    expect(w.find("[role=dialog]").exists()).toBe(true);
    expect((w.services.chat as ChatApiLike).getRaw).toHaveBeenCalledWith("u1", SHA);
    await w.get("[data-action=close]").trigger("click");
    expect(w.find("[role=dialog]").exists()).toBe(false);
  });

  it("question, stop card, restore and report card emit the right events", async () => {
    const asking = mount(TurnView, { props: { turn: blankTurn({ status: "awaiting_input", question: question({ questions: [question().questions[1]!] }) }), busy: false } });
    await asking.get("[data-action=option]").trigger("click");
    await asking.get("[data-action=submit-answers]").trigger("click");
    expect(asking.emitted("answer")?.[0]).toEqual(["u1", [{ question_id: "focus", choice: "技术路线" }]]);

    const paused = mount(TurnView, { props: { turn: blankTurn({ status: "paused", stop: stopInfo() }), busy: false } });
    await paused.get("[data-action=continue]").trigger("click");
    await paused.get("[data-action=finish]").trigger("click");
    expect(paused.emitted("action")).toEqual([["u1", "continue"], ["u1", "finish"]]);

    const cancelled = mount(TurnView, { props: { turn: blankTurn({ status: "cancelled", stop: stopInfo(), canRestore: true }), busy: false } });
    await cancelled.get("[data-action=restore]").trigger("click");
    expect(cancelled.emitted("restore")).toEqual([["u1"]]);

    const report = { artifactId: "report", version: 2, title: "固态电池报告", partial: true, toolBudgetReached: true };
    const done = mountWith(TurnView, { turn: blankTurn({ status: "succeeded", route: "research", report }), busy: false });
    expect(done.get("[data-testid=report-card]").text()).toContain("部分");
    await done.get("[data-action=view-report]").trigger("click");
    expect(done.emitted("panel")?.[0]).toEqual(["report"]);
  });
});

describe("SidePanel", () => {
  it("renders the report through renderMarkdown (no script), lists sources with safe links, shows budget bar", async () => {
    const md = "# 报告\n\n正文 [1]\n\n<script>alert(1)</script><img src=x onerror=alert(2)>\n\n[坏链接](javascript:alert(3))";
    const blob = new Blob([md], { type: "text/markdown" });
    const downloadArtifact = vi.fn(async () => ({ blob, contentType: "text/markdown", etag: "", contentDisposition: "" }));
    const saveBlob = vi.fn();
    const turn = blankTurn({
      status: "succeeded",
      todo: [
        { id: "t1", title: "梳理技术路线", status: "done", budgetShare: 7 },
        { id: "t2", title: "中国量产进度", status: "in_progress" },
        { id: "t3", title: "成本", status: "pending" },
      ],
      budget: { used: 12, limit: 30 },
      sources: [
        { n: 1, title: "固态电池产业观察", url: "https://36kr.com/p/1", site: "36kr.com" },
        { n: 2, title: "全固态 27 年装车", url: "https://finance.sina.com.cn/x", site: "finance.sina.com.cn" },
      ],
      report: { artifactId: "report", version: 3, title: "固态电池报告", partial: false, toolBudgetReached: false },
    });
    const w = mountWith(SidePanel, { turn, tab: "progress" }, { chat: fakeChat({ downloadArtifact }), saveBlob });

    // 进度
    const items = w.findAll("[data-testid=todo-item]");
    expect(items.map((x) => x.attributes("data-status"))).toEqual(["done", "in_progress", "pending"]);
    expect(items[0]!.text()).toContain("梳理技术路线");
    const bar = w.get("[role=progressbar]");
    expect(bar.attributes("aria-valuenow")).toBe("12");
    expect(bar.attributes("aria-valuemax")).toBe("30");
    expect(w.text()).toContain("工具调用 12 / 30");
    expect(downloadArtifact).not.toHaveBeenCalled();

    // 标签
    const tabs = w.findAll("[role=tab]");
    expect(tabs.map((t) => t.text())).toEqual(["进度", "来源 2", "报告"]);
    await tabs[1]!.trigger("click");
    expect(w.emitted("update:tab")?.[0]).toEqual(["sources"]);

    // 来源
    await w.setProps({ tab: "sources" } as never);
    const srcs = w.findAll("[data-testid=source]");
    expect(srcs.map((s) => s.text())).toEqual(["[1] 固态电池产业观察 · 36kr.com", "[2] 全固态 27 年装车 · finance.sina.com.cn"]);
    const a = srcs[0]!.get("a");
    expect(a.attributes("href")).toBe("https://36kr.com/p/1");
    expect(a.attributes("target")).toBe("_blank");
    expect(a.attributes("rel")).toBe("noopener noreferrer");

    // 报告
    await w.setProps({ tab: "report" } as never);
    await flushPromises();
    expect(downloadArtifact).toHaveBeenCalledWith("u1", "report", 3);
    const article = w.get("[data-testid=report]");
    expect(article.find("h1").text()).toBe("报告");
    expect(article.find("script").exists()).toBe(false);
    expect(article.html()).not.toContain("onerror");
    expect(article.html()).not.toContain("javascript:");
    await w.get("[data-action=download-report]").trigger("click");
    expect(saveBlob).toHaveBeenCalledWith(blob, "固态电池报告.md");

    // 再切回报告不重复下载
    await w.setProps({ tab: "progress" } as never);
    await w.setProps({ tab: "report" } as never);
    await flushPromises();
    expect(downloadArtifact).toHaveBeenCalledTimes(1);
  });

  it("shows empty states and a report failure message", async () => {
    const empty = mountWith(SidePanel, { turn: undefined, tab: "progress" });
    expect(empty.text()).toContain("开始一次深度研究");

    const noReport = mountWith(SidePanel, { turn: blankTurn(), tab: "report" });
    expect(noReport.text()).toContain("报告还没有生成");
    const noSources = mountWith(SidePanel, { turn: blankTurn(), tab: "sources" });
    expect(noSources.text()).toContain("还没有来源");

    const downloadArtifact = vi.fn(async () => {
      throw new Error("x");
    });
    const report = { artifactId: "report", version: 1, title: "r", partial: true, toolBudgetReached: true };
    const w = mountWith(SidePanel, { turn: blankTurn({ report }), tab: "report" }, { chat: fakeChat({ downloadArtifact }) });
    await flushPromises();
    expect(w.text()).toContain("报告加载失败");
    expect(w.text()).toContain("部分研究");
  });
});

describe("user-facing views hide internals", () => {
  it("user-facing components never show cost, model names or micro-dollar fields", async () => {
    let s = emptyChat();
    const T = "u-internal-turn-id";
    const events = [
      sev(1, "turn_created", { turn_index: 1, text: "研究固态电池", deep_research: true, model: "kimi-k3", cost_micro: 1234 }, T),
      sev(2, "turn_status", { status: "running", model: "kimi-k3" }, T),
      sev(3, "route", { route: "research", forced: true, model: "kimi-k3", cost_micro: 99 }, T),
      sev(4, "skill_read", { name: "deep-research", model: "kimi-k3" }, T),
      sev(5, "thinking", { text: "先拆分问题", model: "kimi-k3", cost_micro: 55, raw: { request: { model: "kimi-k3" }, response_ref: SHA } }, T),
      sev(6, "todo_updated", { items: [{ id: "todo-x1", title: "技术路线", status: "in_progress", budget_share: 7, cost_micro: 5 }] }, T),
      sev(7, "tool_call", { tool: "web_search", tool_call_id: "call-internal-1", input: { query: "固态电池" }, subtopic_id: "todo-x1", cost_micro: 7 }, T),
      sev(8, "tool_result", {
        tool: "web_search",
        tool_call_id: "call-internal-1",
        ok: true,
        cost_micro: 7,
        model: "kimi-k3",
        preview: { kind: "search", query: "固态电池", results: [{ title: "结果一", url: "https://a.example/1", snippet: "摘要" }] },
        raw: { request: { query: "固态电池" }, response_ref: SHA },
      }, T),
      sev(9, "tool_result", { tool: "web_fetch", ok: true, preview: { kind: "fetch", url: "https://a.example/1", n: 1, title: "来源一", text: "摘录" } }, T),
      sev(10, "budget", { used: 3, limit: 30, cost_micro: 1000 }, T),
      sev(11, "assistant_delta", { text: "结论如下", message_id: "msg-internal-1", model: "kimi-k3" }, T),
      sev(12, "report_ready", { artifact_id: "report", version: 1, title: "报告", cost_micro: 4 }, T),
      sev(13, "turn_status", { status: "succeeded" }, T),
    ];
    for (const e of events) s = applyEvent(s, e);
    const turn = s.turns[0]!;

    const downloadArtifact = vi.fn(async () => ({ blob: new Blob(["# 报告\n\n报告正文"]), contentType: "text/markdown", etag: "", contentDisposition: "" }));
    const tv = mountWith(TurnView, { turn, busy: false }, { chat: fakeChat({ downloadArtifact }) });
    await flushPromises();
    // 已结束的轮次先点开"研究过程"，再展开所有合并行与各行
    await tv.get("[data-testid=chain-toggle]").trigger("click");
    for (const m of tv.findAll("[data-testid=merged-row] > button")) await m.trigger("click");
    for (const h of tv.findAll("[data-testid=step-head]")) await h.trigger("click");
    const htmls: string[] = [tv.html()];
    for (const tab of ["progress", "sources", "report"] as const) {
      const sp = mountWith(SidePanel, { turn, tab }, { chat: fakeChat({ downloadArtifact }) });
      await flushPromises();
      htmls.push(sp.html());
    }
    for (const html of htmls) {
      expect(html).not.toMatch(/kimi/i);
      expect(html).not.toMatch(/micro/i);
      expect(html).not.toContain("$");
      expect(html).not.toContain(T);
      expect(html).not.toContain("call-internal-1");
      expect(html).not.toContain("todo-x1");
      expect(html).not.toContain("msg-internal-1");
      expect(html).not.toContain(SHA);
    }
    // 有报告时对话中显示报告全文（reply 只是摘要，不再重复）
    expect(htmls[0]).toContain("报告正文");
    expect(htmls[0]).not.toContain("结论如下");
    expect(htmls[0]).toContain("结果一");
  });
});

// M4 Plan 14 Task 10：并行 sub-run 的事件交错到达时，步骤行仍按子主题（subtopic_id）归组；用户侧不出现 sub-run ID 与费用。
describe("TurnView groups interleaved sub-run steps by subtopic", () => {
  it("keeps each subtopic's rows together and hides sub-run ids and costs", async () => {
    const T = "turn-par";
    const tc = (seq: number, id: string, sub: string, tool: string, input: Record<string, unknown>) =>
      sev(seq, "tool_call", { tool, tool_call_id: id, input, subtopic_id: sub, subrun_id: sub, cost_micro: 3 }, T);
    const tr = (seq: number, id: string, sub: string, tool: string, preview: Record<string, unknown>) =>
      sev(seq, "tool_result", { tool, tool_call_id: id, ok: true, subtopic_id: sub, subrun_id: sub, cost_micro: 3, preview }, T);
    const events = [
      sev(1, "turn_status", { status: "running" }, T),
      sev(2, "route", { route: "research", forced: true }, T),
      sev(3, "todo_updated", { items: [
        { id: "st1", title: "技术路线", status: "in_progress", budget_share: 10 },
        { id: "st2", title: "成本与供应链", status: "in_progress", budget_share: 10 },
      ] }, T),
      sev(4, "tool_call", { tool: "research_subtopic", tool_call_id: "orch:3", input: { id: "st1" } }, T),
      sev(5, "subtopic", { id: "st1", title: "技术路线", status: "running" }, T),
      sev(6, "subtopic", { id: "st2", title: "成本与供应链", status: "running" }, T),
      tc(7, "st1:1", "st1", "web_search", { query: "硫化物 电解质" }),
      tc(8, "st2:1", "st2", "web_search", { query: "锂价 走势" }),
      tr(9, "st1:1", "st1", "web_search", { kind: "search", query: "硫化物 电解质", results: [] }),
      tc(10, "st2:2", "st2", "web_fetch", { url: "https://cost.example/a" }),
      tc(11, "st1:2", "st1", "web_fetch", { url: "https://tech.example/b" }),
      tr(12, "st2:1", "st2", "web_search", { kind: "search", query: "锂价 走势", results: [] }),
      tr(13, "st1:2", "st1", "web_fetch", { kind: "fetch", url: "https://tech.example/b", n: 1, title: "技术来源", text: "摘录" }),
      tr(14, "st2:2", "st2", "web_fetch", { kind: "fetch", url: "https://cost.example/a", n: 2, title: "成本来源", text: "摘录" }),
    ];
    let s = emptyChat();
    for (const e of events) s = applyEvent(s, e);
    const w = mount(TurnView, { props: { turn: s.turns[0]!, busy: false } });

    expect(w.findAll("[data-testid=subtopic-heading]").map((h) => h.text())).toEqual(["技术路线", "成本与供应链"]);
    const groups = w.findAll("[data-testid=subtopic-group]");
    expect(groups).toHaveLength(2);
    const rowsOf = (g: (typeof groups)[number]) => g.findAll("[data-testid=step-head]").map((h) => h.text());
    expect(rowsOf(groups[0]!)).toHaveLength(2);
    expect(rowsOf(groups[0]!).join("\n")).toContain("硫化物 电解质");
    expect(rowsOf(groups[0]!).join("\n")).toContain("tech.example");
    expect(rowsOf(groups[1]!)).toHaveLength(2);
    expect(rowsOf(groups[1]!).join("\n")).toContain("锂价 走势");
    expect(rowsOf(groups[1]!).join("\n")).toContain("cost.example");
    // research_subtopic 行在分组之前单独显示，不并入第一个子主题
    expect(w.findAll("[data-testid=step-head]")[0]!.text()).toContain("研究子主题");

    for (const h of w.findAll("[data-testid=step-head]")) await h.trigger("click");
    const html = w.html();
    expect(html).not.toMatch(/subrun/i);
    expect(html).not.toMatch(/st[12]:/);
    expect(html).not.toMatch(/"st[12]"|>st[12]</);
    expect(html).not.toMatch(/micro/i);
    expect(html).not.toContain("$");
  });

  it("rows that only carry a subtopic title still group by that title", () => {
    const steps: StepRowData[] = [
      { id: "a", kind: "search", title: "搜索网页", status: "done", subtopic: "技术路线" },
      { id: "b", kind: "search", title: "搜索网页", status: "done", subtopic: "成本" },
      { id: "c", kind: "fetch", title: "阅读网页", status: "done", subtopic: "技术路线" },
    ];
    const w = mount(TurnView, { props: { turn: blankTurn({ steps }), busy: false } });
    expect(w.findAll("[data-testid=subtopic-heading]").map((h) => h.text())).toEqual(["技术路线", "成本"]);
    expect(w.findAll("[data-testid=subtopic-group]")[0]!.findAll("[data-testid=step-head]")).toHaveLength(2);
  });
});

describe("TurnView merges consecutive steps and folds the chain after the turn", () => {
  const fetchRow = (id: string, over: Partial<StepRowData> = {}): StepRowData =>
    ({ id, kind: "fetch", title: `阅读网页 · ${id}.example`, status: "done", text: `${id} 正文`, ...over });
  const thinkRow = (id: string, text: string): StepRowData => ({ id, kind: "thinking", title: "思考", status: "done", text });

  it("renders consecutive fetch rows inside a subtopic as one merged row that expands to each row", async () => {
    const steps = ["f1", "f2", "f3"].map((id) => fetchRow(id, { subtopic: "技术路线" }));
    const w = mount(TurnView, { props: { turn: blankTurn({ steps }), busy: false } });
    const merged = w.findAll("[data-testid=merged-row]");
    expect(merged).toHaveLength(1);
    expect(merged[0]!.text()).toContain("阅读网页 · 3 个网页");
    expect(w.findAll("[data-testid=step-head]")).toHaveLength(0);
    await merged[0]!.get("button").trigger("click");
    expect(w.findAll("[data-testid=step-head]")).toHaveLength(3);
  });

  it("merges consecutive top-level rows too and keeps subtopic groups in place", () => {
    const steps: StepRowData[] = [
      thinkRow("t1", "先想一想"),
      thinkRow("t2", "再想一想"),
      { id: "s1", kind: "search", title: "搜索网页", status: "done", subtopic: "技术路线" },
      fetchRow("f1"),
      fetchRow("f2"),
    ];
    const w = mount(TurnView, { props: { turn: blankTurn({ steps }), busy: false } });
    const order = w.findAll("[data-testid=merged-row], [data-testid=subtopic-group]").map((e) => e.attributes("data-testid") + ":" + (e.attributes("data-kind") ?? ""));
    expect(order).toEqual(["merged-row:thinking", "subtopic-group:", "merged-row:fetch"]);
    expect(w.text()).toContain("思考 · 2 次");
    expect(w.text()).toContain("阅读网页 · 2 个网页");
  });

  it("thinking rows are not expanded while the turn runs", () => {
    const steps: StepRowData[] = [{ id: "t1", kind: "thinking", title: "思考", status: "running", text: "内部推理文本" }];
    const w = mount(TurnView, { props: { turn: blankTurn({ steps, status: "running" }), busy: false } });
    expect(w.get("[data-testid=step-head]").attributes("aria-expanded")).toBe("false");
    expect(w.text()).not.toContain("内部推理文本");
  });

  it("a finished turn shows only the 研究过程 summary until clicked", async () => {
    const steps: StepRowData[] = [searchRow(1), fetchRow("f1"), thinkRow("t1", "想")];
    const turn = blankTurn({ steps, status: "succeeded", startedAt: 1_000, lastEventAt: 253_000 });
    const w = mount(TurnView, { props: { turn, busy: false } });
    const toggle = w.get("[data-testid=chain-toggle]");
    expect(toggle.text()).toContain("研究过程 · 3 步 · 4 分 12 秒");
    expect(toggle.attributes("aria-expanded")).toBe("false");
    expect(w.find(".steps").exists()).toBe(false);
    await toggle.trigger("click");
    expect(w.find(".steps").exists()).toBe(true);
    expect(w.findAll("[data-testid=step-head]")).toHaveLength(3);

    const noTime = mount(TurnView, { props: { turn: blankTurn({ steps, status: "paused" }), busy: false } });
    expect(noTime.get("[data-testid=chain-toggle]").text()).toBe("▸ 研究过程 · 3 步");
    const running = mount(TurnView, { props: { turn: blankTurn({ steps, status: "running" }), busy: false } });
    expect(running.find("[data-testid=chain-toggle]").exists()).toBe(false);
  });

  it("a finished direct-answer turn folds to 过程, not 研究过程", () => {
    const steps: StepRowData[] = [thinkRow("t1", "想")];
    const answer = mount(TurnView, { props: { turn: blankTurn({ steps, status: "succeeded", route: "answer" }), busy: false } });
    expect(answer.get("[data-testid=chain-toggle]").text()).toBe("▸ 过程 · 1 步");
    const research = mount(TurnView, { props: { turn: blankTurn({ steps, status: "succeeded", route: "research" }), busy: false } });
    expect(research.get("[data-testid=chain-toggle]").text()).toBe("▸ 研究过程 · 1 步");
  });
});

describe("TurnView shows the report in the chat with a report card", () => {
  const reportOf = (over: Partial<NonNullable<TurnViewData["report"]>> = {}) => ({
    artifactId: "report",
    version: 2,
    title: "固态电池报告",
    partial: false,
    toolBudgetReached: false,
    ...over,
  });
  const dl = (md: string) => vi.fn(async () => ({ blob: new Blob([md]), contentType: "text/markdown", etag: "", contentDisposition: "" }));

  it("renders the full report in the middle, hides the reply, and the card opens the report tab", async () => {
    const downloadArtifact = dl("# 标题\n\n正文 [1]");
    const turn = blankTurn({ status: "succeeded", route: "research", reply: "摘要：详见报告", report: reportOf({ partial: true, toolBudgetReached: true }) });
    const w = mountWith(TurnView, { turn, busy: false }, { chat: fakeChat({ downloadArtifact }) });
    await flushPromises();
    expect(downloadArtifact).toHaveBeenCalledWith("u1", "report", 2);
    expect(w.get("[data-testid=report-body]").text()).toContain("正文");
    const card = w.get("[data-testid=report-card]");
    expect(card.text()).toContain("固态电池报告");
    expect(card.text()).toContain("部分");
    expect(card.text()).toContain("已达工具额度");
    expect(w.find("[data-testid=reply]").exists()).toBe(false);
    await card.trigger("click");
    expect(w.emitted("panel")?.[0]).toEqual(["report"]);
  });

  it("the card omits the badges when the report is complete", async () => {
    const w = mountWith(TurnView, { turn: blankTurn({ status: "succeeded", route: "research", report: reportOf() }), busy: false }, { chat: fakeChat({ downloadArtifact: dl("# 标题") }) });
    await flushPromises();
    const card = w.get("[data-testid=report-card]");
    expect(card.text()).not.toContain("部分");
    expect(card.text()).not.toContain("已达工具额度");
  });

  it("downloads a report once for the in-chat body and the side panel (shared, failures retried)", async () => {
    const downloadArtifact = dl("# 标题\n\n正文");
    const chat = fakeChat({ downloadArtifact });
    const turn = blankTurn({ status: "succeeded", route: "research", report: reportOf() });
    const tv = mountWith(TurnView, { turn, busy: false }, { chat });
    const sp = mountWith(SidePanel, { turn, tab: "report" }, { chat });
    await flushPromises();
    expect(tv.get("[data-testid=report-body]").text()).toContain("正文");
    expect(sp.get("[data-testid=report]").text()).toContain("正文");
    expect(downloadArtifact).toHaveBeenCalledTimes(1);

    // 失败不缓存：下一个使用者重新下载。
    let fail = true;
    const flaky = vi.fn(async () => {
      if (fail) throw new Error("x");
      return { blob: new Blob(["# 重试"]), contentType: "text/markdown", etag: "", contentDisposition: "" };
    });
    const chat2 = fakeChat({ downloadArtifact: flaky });
    const first = mountWith(SidePanel, { turn, tab: "report" }, { chat: chat2 });
    await flushPromises();
    expect(first.text()).toContain("报告加载失败");
    fail = false;
    const second = mountWith(SidePanel, { turn, tab: "report" }, { chat: chat2 });
    await flushPromises();
    expect(second.get("[data-testid=report]").text()).toContain("重试");
    expect(flaky).toHaveBeenCalledTimes(2);
  });

  it("sanitizes the in-chat report like the side panel (no script, handlers or javascript: links)", async () => {
    const md = "# 报告\n\n正文\n\n<script>alert(1)</script><img src=x onerror=alert(2)>\n\n[坏链接](javascript:alert(3))";
    const w = mountWith(TurnView, { turn: blankTurn({ status: "succeeded", route: "research", report: reportOf() }), busy: false }, { chat: fakeChat({ downloadArtifact: dl(md) }) });
    await flushPromises();
    const body = w.get("[data-testid=report-body]").get("article");
    expect(body.find("h1").text()).toBe("报告");
    expect(body.find("script").exists()).toBe(false);
    expect(body.html()).not.toContain("onerror");
    expect(body.html()).not.toContain("javascript:");
  });

  it("shows a ticking writing row while the lead agent writes the report", async () => {
    vi.useFakeTimers();
    try {
      vi.setSystemTime(new Date("2026-10-07T00:00:10Z"));
      const lastEventAt = new Date("2026-10-07T00:00:05Z").getTime();
      const turn = blankTurn({
        status: "running",
        route: "research",
        subtopics: [{ id: "s1", title: "技术路线", status: "done" }],
        steps: [searchRow(1, { status: "done" })],
        lastEventAt,
      });
      const w = mountWith(TurnView, { turn, busy: false });
      const row = w.get("[data-testid=writing-report]");
      expect(row.text()).toBe("✍️ 正在整理资料并撰写报告… 5 秒");
      await vi.advanceTimersByTimeAsync(3000);
      expect(w.get("[data-testid=writing-report]").text()).toBe("✍️ 正在整理资料并撰写报告… 8 秒");

      await w.setProps({ turn: { ...turn, steps: [searchRow(1, { status: "running" })] } } as never);
      expect(w.find("[data-testid=writing-report]").exists()).toBe(false);
    } finally {
      vi.useRealTimers();
    }
  });

  it("shows a ticking 正在停止… row while stopping, hidden once paused or the stop card arrives", async () => {
    vi.useFakeTimers();
    try {
      vi.setSystemTime(new Date("2026-10-07T00:00:10Z"));
      const stoppingSince = new Date("2026-10-07T00:00:07Z").getTime();
      const turn = blankTurn({ status: "stopping", route: "research", steps: [searchRow(1)], stoppingSince, lastEventAt: stoppingSince });
      const w = mountWith(TurnView, { turn, busy: false });
      const row = w.get("[data-testid=stopping]");
      expect(row.text()).toBe("正在停止… 3 秒");
      expect(row.attributes("role")).toBe("status");
      expect(row.get("[aria-hidden=true]").text()).toBe("3 秒");
      await vi.advanceTimersByTimeAsync(2000);
      expect(w.get("[data-testid=stopping]").text()).toBe("正在停止… 5 秒");

      const stop = { todo: [], subtopicsDone: 0, subtopicsTotal: 0, sources: 0, budget: { used: 1, limit: 30 }, findings: "f", canFinish: false };
      await w.setProps({ turn: { ...turn, stop } } as never);
      expect(w.find("[data-testid=stopping]").exists()).toBe(false);
      await w.setProps({ turn: { ...turn, status: "paused", stoppingSince: undefined } } as never);
      expect(w.find("[data-testid=stopping]").exists()).toBe(false);
    } finally {
      vi.useRealTimers();
    }
  });
});

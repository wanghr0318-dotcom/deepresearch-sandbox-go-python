import { describe, expect, it } from "vitest";
import { renderMarkdown } from "./markdown";

// E27：报告 Markdown 中的主动内容渲染后不可执行。
const HOSTILE = [
  "# 报告",
  "",
  "<script>window.__pwned = 1</script>",
  "",
  '<img src="x" onerror="window.__pwned = 2">',
  "",
  "[点我](javascript:window.__pwned=3)",
  "",
  '<a href="JaVaScRiPt:window.__pwned=4">大小写</a>',
  "",
  '<a href="java&#x09;script:window.__pwned=5">实体</a>',
  "",
  '<a href=" javascript:window.__pwned=6">空格</a>',
  "",
  '<iframe src="https://evil.example/"></iframe>',
  "",
  '<object data="x.swf"></object><embed src="x.swf">',
  "",
  "<style>body { display: none }</style>",
  "",
  '<svg onload="window.__pwned=7"><circle r="1"/></svg>',
  "",
  '<div onclick="window.__pwned=8" style="position:fixed">z</div>',
  "",
  '<form action="https://evil.example/"><input name="x"><button formaction="javascript:alert(1)">go</button></form>',
  "",
  '<a href="data:text/html,<script>alert(1)</script>">data</a>',
  "",
  "[正常链接](https://example.com/report)",
].join("\n");

function container(html: string): HTMLElement {
  const div = document.createElement("div");
  div.innerHTML = html;
  return div;
}

function allElements(root: HTMLElement): Element[] {
  return [root, ...Array.from(root.querySelectorAll("*"))];
}

describe("renderMarkdown (E27)", () => {
  it("removes script, iframe, object, embed, style, svg and form elements", () => {
    const root = container(renderMarkdown(HOSTILE));
    for (const tag of ["script", "iframe", "object", "embed", "style", "svg", "form", "input", "button"]) {
      expect(root.querySelector(tag), tag).toBeNull();
    }
  });

  it("removes every event-handler attribute and style attribute", () => {
    const root = container(renderMarkdown(HOSTILE));
    for (const el of allElements(root)) {
      for (const attr of Array.from(el.attributes)) {
        expect(attr.name.toLowerCase().startsWith("on"), `${el.tagName} ${attr.name}`).toBe(false);
        expect(attr.name.toLowerCase()).not.toBe("style");
      }
    }
  });

  it("removes javascript: and data: URLs", () => {
    const root = container(renderMarkdown(HOSTILE));
    for (const el of allElements(root)) {
      for (const name of ["href", "src", "action", "formaction", "xlink:href"]) {
        const v = el.getAttribute(name);
        if (v === null) continue;
        const norm = v.replace(/[\s\u0000-\u001f]/g, "").toLowerCase();
        expect(norm.startsWith("javascript:"), `${el.tagName} ${name}=${v}`).toBe(false);
        if (el.tagName === "A") expect(norm.startsWith("data:"), `${name}=${v}`).toBe(false);
      }
    }
    const html = renderMarkdown(HOSTILE).toLowerCase();
    expect(html).not.toContain("javascript:");
    expect(html).not.toContain("<script");
    expect(html).not.toContain("onerror");
    expect(html).not.toContain("onload");
  });

  it("keeps ordinary content and gives links rel=noopener noreferrer", () => {
    const root = container(renderMarkdown(HOSTILE));
    expect(root.querySelector("h1")?.textContent).toBe("报告");
    const link = Array.from(root.querySelectorAll("a")).find((a) => a.textContent === "正常链接");
    expect(link?.getAttribute("href")).toBe("https://example.com/report");
    for (const a of Array.from(root.querySelectorAll("a[href]"))) {
      expect(a.getAttribute("rel")).toBe("noopener noreferrer");
      expect(a.getAttribute("target")).toBe("_blank");
    }
  });

  it("inserting the output into the live document executes nothing", () => {
    const w = window as unknown as { __pwned?: number };
    delete w.__pwned;
    const root = container(renderMarkdown(HOSTILE));
    document.body.appendChild(root);
    for (const el of allElements(root)) {
      el.dispatchEvent(new Event("click", { bubbles: true }));
      el.dispatchEvent(new Event("error"));
      el.dispatchEvent(new Event("load"));
    }
    root.remove();
    expect(w.__pwned).toBeUndefined();
  });

  it("renders plain markdown structure", () => {
    const root = container(renderMarkdown("## 小节\n\n- a\n- b\n\n`code`\n\n| x | y |\n|---|---|\n| 1 | 2 |"));
    expect(root.querySelector("h2")?.textContent).toBe("小节");
    expect(root.querySelectorAll("li")).toHaveLength(2);
    expect(root.querySelector("code")?.textContent).toBe("code");
    expect(root.querySelector("table")).not.toBeNull();
  });
});

// 报告 Markdown 的安全渲染（规格 §15.4、Plan 10 全局约束"渲染安全"）：
// marked 把 Markdown 转成 HTML，DOMPurify 再清理，结果才允许经 v-html 插入。
// - 禁止 <script>、<iframe>、<object>、<embed>、<style>、表单控件、<svg>/<math> 等主动内容；
// - 去掉所有事件属性（on*）与 style 属性；
// - 只允许 http(s)/mailto/相对链接，javascript:、data:、vbscript: 等 URL 一律去掉；
// - 链接一律加 rel="noopener noreferrer" 并在新标签页打开。

import createDOMPurify from "dompurify";
import { Marked } from "marked";

const FORBID_TAGS = [
  "script",
  "iframe",
  "frame",
  "frameset",
  "object",
  "embed",
  "applet",
  "style",
  "link",
  "meta",
  "base",
  "form",
  "input",
  "button",
  "textarea",
  "select",
  "option",
  "svg",
  "math",
  "template",
  "noscript",
];

const FORBID_ATTR = ["style", "srcset", "formaction", "action", "xlink:href", "background", "ping"];

// 只允许 http(s)、mailto 与不带协议的相对地址（含 #锚点）。
const SAFE_URI = /^(?:(?:https?|mailto):|[^a-z]|[a-z+.-]+(?:[^a-z+.\-:]|$))/i;

const marked = new Marked({ gfm: true, breaks: false, async: false });

type Purifier = ReturnType<typeof createDOMPurify>;
let purifier: Purifier | null = null;

function getPurifier(): Purifier {
  if (purifier) return purifier;
  // 独立实例：hook 不影响其他使用 DOMPurify 的代码。
  const p = createDOMPurify(window);
  p.addHook("afterSanitizeAttributes", (node) => {
    if (node.nodeName === "A") {
      const el = node as Element;
      if (el.hasAttribute("href")) {
        el.setAttribute("target", "_blank");
        el.setAttribute("rel", "noopener noreferrer");
      } else {
        el.removeAttribute("target");
      }
    }
  });
  purifier = p;
  return p;
}

/** 把不可信的 Markdown 渲染为已清理的 HTML 字符串。 */
export function renderMarkdown(src: string): string {
  const raw = marked.parse(src) as string;
  return getPurifier().sanitize(raw, {
    USE_PROFILES: { html: true },
    FORBID_TAGS,
    FORBID_ATTR,
    ALLOW_DATA_ATTR: false,
    ALLOW_UNKNOWN_PROTOCOLS: false,
    ALLOWED_URI_REGEXP: SAFE_URI,
    RETURN_TRUSTED_TYPE: false,
  }) as string;
}

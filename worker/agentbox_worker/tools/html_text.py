"""HTML → 正文文本（只用标准库，输出确定）。

1. 解析为简单的元素树（容错：未闭合标签在父元素结束时一并结束）。
2. 去掉站点外壳：<nav>/<header>/<footer>/<aside>/<form>、脚本与样式，以及 class/id/role 看起来是
   导航、菜单、面包屑、页脚、侧栏、Cookie 提示或广告的元素；链接文字占绝大部分的列表与块（菜单）。
3. 有 <article>/<main>/[role=main] 时取其中文字最多的一个；否则取段落文字最密集的块（各段落
   ≥ 25 字的文字量计入其父元素）；都不足时退回去壳后的全页文本；去壳后为空时退回原始全页文本。
"""

from __future__ import annotations

import re
from collections.abc import Iterator
from dataclasses import dataclass, field
from html.parser import HTMLParser

# 内容永远不要的标签
_ALWAYS_SKIP = frozenset(
    {"script", "style", "noscript", "template", "svg", "head", "iframe", "object", "canvas"}
)
# 站点外壳（结构标签）
_CHROME_TAGS = frozenset({"nav", "header", "footer", "aside", "form", "button", "select", "dialog"})
_CHROME_ROLES = frozenset(
    {"navigation", "banner", "contentinfo", "complementary", "search", "menu", "menubar", "dialog"}
)
_CHROME_WORDS = re.compile(
    r"(?:^|[-_ ])(?:nav|navbar|navigation|menu|menubar|breadcrumbs?|crumbs?|footer|sidebar|"
    r"side-bar|cookies?|consent|gdpr|ads?|advert\w*|sponsor\w*|banner|share|social|toolbar|"
    r"topbar|skip-link|related|recommend\w*|popup|modal|subscribe|newsletter)(?:$|[-_ ])",
    re.IGNORECASE,
)
# 不按 class/id 判为外壳的标签（正文容器本身）
_NEVER_CHROME = frozenset({"html", "body", "main", "article"})
_VOID = frozenset(
    {
        "area",
        "base",
        "br",
        "col",
        "embed",
        "hr",
        "img",
        "input",
        "link",
        "meta",
        "param",
        "source",
        "track",
        "wbr",
    }
)
_INLINE = frozenset(
    {"a", "abbr", "b", "cite", "code", "em", "font", "i", "mark", "q", "s", "small", "span"}
    | {"strong", "sub", "sup", "time", "u"}
)
_PARA_TAGS = frozenset({"p", "pre", "blockquote", "h1", "h2", "h3", "h4", "h5", "h6", "li", "td"})
_MENU_TAGS = frozenset({"ul", "ol", "div", "section", "table", "p", "span"})

MAX_DEPTH = 200  # 元素树的最大深度
PARA_MIN_CHARS = 25  # 计入密度的段落最短字数
MAIN_MIN_CHARS = 20  # 正文地标中的文字少于此数时不采用
DENSE_MIN_CHARS = 60  # 最密集块的段落文字量少于此数时不采用


@dataclass
class _Node:
    tag: str
    attrs: dict[str, str] = field(default_factory=dict)
    children: list[_Node | str] = field(default_factory=list)


class _TreeBuilder(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.root = _Node("#root")
        self._stack: list[_Node] = [self.root]

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        node = _Node(tag, {k: v or "" for k, v in attrs})
        self._stack[-1].children.append(node)
        # 嵌套过深（多为未闭合的标签）时不再下沉：之后的内容挂在当前元素下，递归深度有界
        if tag not in _VOID and len(self._stack) < MAX_DEPTH:
            self._stack.append(node)

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self._stack[-1].children.append(_Node(tag, {k: v or "" for k, v in attrs}))

    def handle_endtag(self, tag: str) -> None:
        for i in range(len(self._stack) - 1, 0, -1):
            if self._stack[i].tag == tag:
                del self._stack[i:]
                return

    def handle_data(self, data: str) -> None:
        if data:
            self._stack[-1].children.append(data)


def html_to_text(html: str) -> str:
    """HTML 的正文文本（空白已压缩为单个空格）。"""
    builder = _TreeBuilder()
    builder.feed(html)
    builder.close()
    root = builder.root
    full = _text(_prune(root, level=0)[0])
    structural = _prune(root, level=1)[0]  # 结构外壳与菜单
    cleaned = _prune(root, level=2)[0]  # 再按 class/id 判断
    s_text, c_text = _text(structural), _text(cleaned)
    if len(c_text) * 5 < len(s_text):  # class/id 误伤了正文容器（如 "sidebar-layout"）：不采用
        cleaned, c_text = structural, s_text
    if not c_text:
        return full
    landmarks = [n for n in _walk(cleaned) if _is_main(n)]
    if landmarks:
        best = max((_text(n) for n in landmarks), key=len)  # 并列时取文档中靠前的
        if len(best) >= MAIN_MIN_CHARS:
            return best
    dense = _densest(cleaned)
    return dense if dense is not None else c_text


def _prune(node: _Node, *, level: int) -> tuple[_Node, int, int, int]:
    """去掉外壳后的副本（不修改原树）及其 (文字数, 链接文字数, 链接数)。

    level 0：只去脚本、样式等；1：再去结构外壳与菜单式的块；2：再按 class/id 判断。"""
    out = _Node(node.tag, node.attrs)
    chars = link_chars = links = 0
    for child in node.children:
        if isinstance(child, str):
            out.children.append(child)
            chars += len("".join(child.split()))
            continue
        if child.tag in _ALWAYS_SKIP or (level and _is_chrome(child, by_name=level >= 2)):
            continue
        sub, c, lc, ln = _prune(child, level=level)
        if child.tag == "a":
            lc, ln = c, ln + 1
        if level and child.tag in _MENU_TAGS and ln >= 3 and c and lc >= 0.7 * c:
            continue  # 菜单：至少 3 个链接，且链接文字占 70% 以上
        out.children.append(sub)
        chars, link_chars, links = chars + c, link_chars + lc, links + ln
    return out, chars, link_chars, links


def _is_chrome(node: _Node, *, by_name: bool) -> bool:
    if node.tag in _CHROME_TAGS:
        return True
    if node.attrs.get("role", "").strip().lower() in _CHROME_ROLES:
        return True
    if not by_name or node.tag in _NEVER_CHROME:
        return False
    if node.attrs.get("aria-hidden", "").lower() == "true" or "hidden" in node.attrs:
        return True
    names = (node.attrs.get("class", "") + " " + node.attrs.get("id", "")).split()
    return any(_CHROME_WORDS.search(name) for name in names)


def _is_main(node: _Node) -> bool:
    return node.tag in ("article", "main") or node.attrs.get("role", "").lower() == "main"


def _densest(root: _Node) -> str | None:
    """段落文字最密集的元素的文本：每个 ≥ PARA_MIN_CHARS 字的段落把字数计入父元素、一半计入
    祖父元素（段落各自包在一层 <div> 里时由外层容器胜出）；最高分不足 DENSE_MIN_CHARS 时 None。
    同分取文档中靠前的。"""
    scores: dict[int, int] = {}
    nodes: dict[int, _Node] = {}
    order: dict[int, int] = {}
    parents: dict[int, _Node] = {}
    for k, node in enumerate(_walk(root)):
        order[id(node)] = k
        nodes[id(node)] = node
        for child in node.children:
            if isinstance(child, _Node):
                parents[id(child)] = node
        if node.tag not in _PARA_TAGS or (n := len(_text(node))) < PARA_MIN_CHARS:
            continue
        parent = parents.get(id(node))
        if parent is None:
            continue
        scores[id(parent)] = scores.get(id(parent), 0) + n
        if (grand := parents.get(id(parent))) is not None:
            scores[id(grand)] = scores.get(id(grand), 0) + n // 2
    if not scores:
        return None
    key = min(scores, key=lambda i: (-scores[i], order[i]))
    if scores[key] < DENSE_MIN_CHARS:
        return None
    return _text(nodes[key])


def _walk(node: _Node) -> Iterator[_Node]:
    """文档序（先序）遍历元素。"""
    stack = [node]
    while stack:
        n = stack.pop()
        yield n
        stack.extend(c for c in reversed(n.children) if isinstance(c, _Node))


def _text(node: _Node) -> str:
    parts: list[str] = []
    stack: list[_Node | str] = [node]
    while stack:
        item = stack.pop()
        if isinstance(item, str):
            parts.append(item)
        else:
            if item.tag not in _INLINE:
                parts.append(" ")  # 块级元素边界视为空白；行内元素（如 <b>）不切开文字
            stack.extend(reversed(item.children))
    return " ".join("".join(parts).split())

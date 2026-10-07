"""识别人机验证/拦截页：这类页面的"正文"是验证提示，不能交给模型或作为来源。"""

from __future__ import annotations

import re

# 只在短正文中判定：长文章即使提到这些词也不是拦截页。
_MAX_CHARS = 1200
# 泛用词（教程、排错文章也会用）只在很短的正文里才算，且不看标题。
_WEAK_MAX_CHARS = 300
# 验证页特有的句子：正文或标题命中即判定。
_STRONG = re.compile(
    r"正在进行安全检测|请完成安全验证|"
    r"just a moment|checking your browser|cf-browser-verification|enable javascript and cookies|"
    r"verify you are human",
    re.IGNORECASE,
)
_WEAK = re.compile(
    r"安全验证|人机验证|访问验证|滑动验证|captcha|access denied|attention required",
    re.IGNORECASE,
)


def is_blocked_page(text: str, title: str) -> bool:
    body = (text or "").strip()
    if not body or len(body) > _MAX_CHARS:
        return False
    if _STRONG.search(body) or _STRONG.search(title or ""):
        return True
    return len(body) <= _WEAK_MAX_CHARS and bool(_WEAK.search(body))

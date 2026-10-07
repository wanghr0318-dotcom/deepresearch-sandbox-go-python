"""识别人机验证/拦截页：这类页面的"正文"是验证提示，不能交给模型或作为来源。"""

from __future__ import annotations

import re

# 只在短正文中判定：长文章即使提到这些词也不是拦截页。
_MAX_CHARS = 1200
_PATTERNS = re.compile(
    r"正在进行安全检测|请完成安全验证|安全验证|人机验证|访问验证|滑动验证|"
    r"just a moment|checking your browser|cf-browser-verification|enable javascript and cookies|"
    r"attention required|captcha|verify you are human|access denied",
    re.IGNORECASE,
)


def is_blocked_page(text: str, title: str) -> bool:
    body = (text or "").strip()
    if not body or len(body) > _MAX_CHARS:
        return False
    return bool(_PATTERNS.search(body) or _PATTERNS.search(title or ""))

"""人机验证/拦截页识别：短正文命中验证提示才判定；长文章提到这些词不算。"""

from agentbox_worker.tools.blocked import is_blocked_page


def test_detects_challenge_pages():
    assert is_blocked_page(
        "火山引擎 正在进行安全检测... 为保障您的访问安全，"
        "系统正在检测当前网络环境，该过程通常需要几秒钟，请耐心等待",
        "",
    )
    assert is_blocked_page(
        "Just a moment...\nEnable JavaScript and cookies to continue", "Just a moment..."
    )
    assert is_blocked_page("Checking your browser before accessing example.com", "")
    assert is_blocked_page("请完成安全验证后继续访问", "安全验证")


def test_keeps_normal_pages():
    long_news = "国家市场监督管理总局发布新的安全检测标准。" * 80  # 正常长文提到"安全检测"
    assert not is_blocked_page(long_news, "新标准发布")
    assert not is_blocked_page("固态电池量产时间表：2027 年小批量装车。", "固态电池")
    assert not is_blocked_page("", "")

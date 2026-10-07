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


def test_generic_words_only_count_in_short_bodies():
    howto = "打开微信，进入设置中的账号与安全，找到安全验证选项，按提示操作即可关闭。" * 15
    assert 300 < len(howto) <= 1200
    assert not is_blocked_page(howto, "如何关闭微信安全验证")
    troubleshoot = "服务器返回 403 时，先检查权限配置与访问令牌是否过期，再查看网关日志。" * 12
    assert 300 < len(troubleshoot) <= 1200
    assert not is_blocked_page(troubleshoot, "Access Denied 错误排查")
    assert is_blocked_page("Access Denied\nYou don't have permission to access this server.", "")


def test_challenge_title_blocks_short_pages():
    assert is_blocked_page("请稍候，正在为您加载页面。", "Just a moment...")

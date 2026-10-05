"""以真实子进程运行 `python -m sim_worker`：协议、退出码、print 改道与有界退出。

进程控制属于必须在 Linux 上验证的部分；Windows 上的结果仅供参考（见计划"平台覆盖"）。
"""

import json
import os
import subprocess
import sys
import threading
import time

from agentbox_worker.transport import MAX_FRAME_BYTES

# 固定子进程 stderr 编码：Windows 上默认使用本地代码页（如 GBK），断言中文日志会失败。
ENV = {**os.environ, "AGENTBOX_WORKER_SHUTDOWN_TIMEOUT": "1", "PYTHONIOENCODING": "utf-8"}
CANCEL = b'{"type":"cancel","v":1,"attempt_id":"a-1","reason":"user","grace_ms":0}\n'


def init_line(tmp_path, steps: list[dict]) -> bytes:
    init = {
        "type": "init",
        "bootstrap": 1,
        "protocol_versions": [1],
        "mode": "task",
        "task_id": "t-1",
        "attempt_id": "a-1",
        "attempt_no": 1,
        "out_dir": str(tmp_path),
        "config": {"steps": steps, "summary": "完成", "outputs": []},
    }
    return json.dumps(init).encode() + b"\n"


def start() -> subprocess.Popen[bytes]:
    return subprocess.Popen(
        [sys.executable, "-m", "sim_worker"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=ENV,
    )


def stop(proc: subprocess.Popen[bytes]) -> None:
    if proc.poll() is None:
        proc.kill()
        proc.wait()
    for stream in (proc.stdin, proc.stdout, proc.stderr):
        if stream is not None and not stream.closed:
            try:
                stream.close()
            except OSError:
                pass


def test_process_result_and_print_goes_to_stderr(tmp_path):
    proc = start()
    try:
        steps = [
            {"op": "progress", "step_id": "s1", "kind": "step_started", "message": "start"},
            {"op": "print", "message": "printed-marker"},
        ]
        proc.stdin.write(init_line(tmp_path, steps))
        proc.stdin.flush()
        code = proc.wait(timeout=20)  # 先等待退出再读输出：Worker 若不退出，测试超时失败而不是挂起
        events = [json.loads(line) for line in proc.stdout]
        stderr = proc.stderr.read().decode("utf-8", errors="replace")
    finally:
        stop(proc)
    assert code == 0
    assert [e["type"] for e in events] == ["ready", "progress", "result"]
    assert "printed-marker" in stderr


def test_process_exits_while_host_keeps_stdin_open(tmp_path):
    proc = start()
    try:
        proc.stdin.write(init_line(tmp_path, []))
        proc.stdin.flush()  # stdin 保持打开，Worker 仍须在结束后退出
        code = proc.wait(timeout=20)
        events = [json.loads(line) for line in proc.stdout]
    finally:
        stop(proc)
    assert code == 0 and events[-1]["type"] == "result"


def test_process_exits_after_cancel_when_host_stops_reading_stdout(tmp_path):
    proc = start()
    try:
        proc.stdin.write(init_line(tmp_path, [{"op": "flood", "count": 5000, "size": 1000}]))
        proc.stdin.flush()
        time.sleep(1.0)  # 不读取 stdout：Worker 写满管道后阻塞在写出上
        proc.stdin.write(CANCEL)
        proc.stdin.flush()
        started = time.monotonic()
        code = proc.wait(timeout=20)
        elapsed = time.monotonic() - started
        stderr = proc.stderr.read().decode("utf-8", errors="replace")
    finally:
        stop(proc)
    assert code == 0
    assert elapsed < 10
    assert "未写出" in stderr


def test_process_fails_fast_when_stdout_is_closed(tmp_path):
    proc = start()
    proc.stdout.close()
    try:
        proc.stdin.write(init_line(tmp_path, [{"op": "flood", "count": 5000, "size": 1000}]))
        proc.stdin.flush()
        started = time.monotonic()
        code = proc.wait(timeout=20)
        elapsed = time.monotonic() - started
    finally:
        stop(proc)
    assert code == 1
    assert elapsed < 10


def test_process_rejects_oversized_input_line(tmp_path):
    proc = start()

    def feed() -> None:
        try:
            proc.stdin.write(b"a" * (MAX_FRAME_BYTES + 10))
            proc.stdin.flush()
        except (OSError, ValueError):
            pass  # Worker 已在超限后退出，或管道已被关闭

    writer = threading.Thread(target=feed, daemon=True)
    writer.start()
    try:
        code = proc.wait(timeout=20)
        out = proc.stdout.read()
    finally:
        stop(proc)
    assert code == 1
    assert out == b""

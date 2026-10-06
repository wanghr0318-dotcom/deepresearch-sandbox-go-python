#!/usr/bin/env python3
"""Plan 14 Task 12 服务器真实验收 2–5（用户路径 + 运维 inspect）。在服务器上以 root 运行：
    sudo python3 accept.py --creds cred.json --out acc
与 run.py 同目录（复用其 Client 与指标函数）。不打印 token、cookie、口令。"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from run import EVIDENCE, Client, metrics, rid, ts  # noqa: E402

Q_PARALLEL = ("对比美国、欧盟和中国在 2024–2025 年对生成式 AI 的监管做法：分别说明三地的主要法规或政策文件、"
              "适用对象与生效时间。请按三个地区分别研究后汇总。")
Q_CONTROL = ("比较三种家用储能方案：特斯拉 Powerwall 3、比亚迪 Battery-Box、华为 LUNA2000 的容量、功率、"
             "保修与价格。请按三个产品分别研究后汇总。")

Q_FINISH = ("研究三个问题：一、2025 年全球数据中心用电量的主要估计（IEA 等机构）；二、英伟达 GB200 NVL72 机柜的功耗；"
            "三、爱尔兰对新建数据中心并网的限制政策。三个问题分别研究后汇总。")


def task(c: Client, tid: str) -> dict:
    return c.json("GET", f"/tasks/{tid}", admin=True)


def inspect(c: Client, tid: str) -> dict:
    return c.json("GET", f"/tasks/{tid}/inspect", admin=True)


def wait(c: Client, tid: str, statuses: set[str], limit: float = 1800) -> str:
    t0 = time.time()
    while time.time() - t0 < limit:
        st = task(c, tid)["status"]
        if st in statuses:
            return st
        time.sleep(3)
    raise RuntimeError(f"{tid} 未到 {statuses}")


def send(c: Client, text: str, title: str) -> tuple[str, str]:
    sid = c.json("POST", "/sessions", {"request_id": rid(), "title": title}, expect=(201,))["session_id"]
    tid = c.json("POST", f"/sessions/{sid}/messages",
                 {"request_id": rid(), "text": text, "deep_research": True}, expect=(202,))["turn_id"]
    return sid, tid


def ctl(c: Client, tid: str, action: str) -> None:
    c.json("POST", f"/turns/{tid}/{action}", {"request_id": rid()}, expect=(200, 202))


def answer_if_asked(c: Client, tid: str) -> list | None:
    """turn 处于 awaiting_input 时用每题第一个选项回答（读取任务事件中的最后一个 ask_user）。"""
    t = task(c, tid)
    if t["status"] != "paused" or t.get("status_reason") != "awaiting_input":
        return None
    asks = [e for e in c.sse(f"/tasks/{tid}/events")
            if (e.get("payload") or {}).get("kind") == "ask_user"]
    data = asks[-1]["payload"]["data"]
    answers = [{"question_id": q["id"], "choice": q["options"][0]} for q in data["questions"]]
    c.json("POST", f"/turns/{tid}/answer", {"request_id": rid(), "answers": answers}, expect=(200, 202))
    return answers


def collect(c: Client, out: Path, tag: str, tid: str, extra: dict) -> dict:
    d = out / tag
    (d / "blobs").mkdir(parents=True, exist_ok=True)
    ins = inspect(c, tid)
    ev = c.sse(f"/tasks/{tid}/events")
    (d / "inspect.json").write_text(json.dumps(ins, ensure_ascii=False, indent=1))
    (d / "events.json").write_text(json.dumps(ev, ensure_ascii=False, indent=1))
    st, b = c.call("GET", f"/tasks/{tid}/artifacts/report", admin=True)
    report = b.decode() if st == 200 else ""
    (d / "report.md").write_text(report)
    evidence, located = {}, {}
    for line in report.split("\n"):
        if m := EVIDENCE.match(line):
            evidence[int(m.group(1))] = {"url": m.group(2), "sha256": m.group(3)}
    for n, e in evidence.items():
        s, _ = c.call("GET", f"/turns/{tid}/raw/{e['sha256']}", admin=True)
        located[n] = s == 200
    rec = metrics({"id": tag, "lang": "zh"}, "user", tid, ins, ev, report, evidence, located)
    rec.update(extra)
    (out / f"{tag}.json").write_text(json.dumps(rec, ensure_ascii=False, indent=1))
    return rec


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--creds", type=Path, required=True)
    ap.add_argument("--out", type=Path, default=Path("acc"))
    ap.add_argument("--tag", default="a4")
    ap.add_argument("--only", choices=["parallel", "control", "finish"], default=None)
    ap.add_argument("--turn", default=None)
    ap.add_argument("--session", default=None)
    a = ap.parse_args()
    c = Client("49.235.41.14", Path("/var/lib/agentbox/api.token"))
    c.json("POST", "/auth/login", json.loads(a.creds.read_text()))
    a.out.mkdir(parents=True, exist_ok=True)

    if a.only in (None, "parallel"):
        sid, tid = send(c, Q_PARALLEL, "验收 并行研究")
        print(f"[A2] turn {tid[-6:]}", flush=True)
        st = wait(c, tid, {"succeeded", "failed", "cancelled", "paused"})
        rec = collect(c, a.out, "a2", tid, {"final": st, "session_id": sid})
        print(f"[A2] {st} wall {rec['wall_s']} overlap {rec['overlap']} subruns "
              f"{[(s['id'], s['status']) for s in rec['subruns']]} tools {rec['tool_calls_used']}/"
              f"{rec['tool_call_limit']} located {rec['located']}/{rec['cited_n']}", flush=True)

    if a.only in (None, "control"):
        if a.turn:
            sid, tid = a.session, a.turn
        else:
            sid, tid = send(c, Q_CONTROL, "验收 停止继续与立即写报告")
        print(f"[A3] turn {tid[-6:]}", flush=True)
        log: dict = {"session_id": sid}
        if (ans := answer_if_asked(c, tid)) is not None:
            log["answered"] = ans
            print(f"[A3] 回答提问 {ans}", flush=True)
        # 1) 第一个子主题完成、其余仍在运行时停止
        while True:
            subs = inspect(c, tid)["subruns"]
            done = [s["subrun_id"] for s in subs if s["status"] == "completed"]
            running = [s["subrun_id"] for s in subs if s["status"] == "started"]
            if done and running:
                break
            if task(c, tid)["status"] in ("succeeded", "failed", "cancelled"):
                raise RuntimeError("研究在停止前已结束")
            time.sleep(2)
        log["stop1_at"] = time.time()
        log["done_at_stop1"] = done
        ctl(c, tid, "stop")
        print(f"[A3] stop（已完成 {done}，运行中 {running}）", flush=True)
        log["stop1_status"] = wait(c, tid, {"paused", "succeeded", "failed", "cancelled"}, 300)
        log["paused_after_s"] = round(time.time() - log["stop1_at"], 1)
        before = {s["subrun_id"]: s["calls"] for s in inspect(c, tid)["subruns"]}
        log["calls_at_pause"] = before
        # 2) 继续；约 20 s 后再停止，然后立即写报告
        log["continue_at"] = time.time()
        ctl(c, tid, "continue")
        print("[A3] continue", flush=True)
        time.sleep(20)
        subs = inspect(c, tid)["subruns"]
        log["running_at_stop2"] = [s["subrun_id"] for s in subs if s["status"] == "started"]
        ctl(c, tid, "stop")
        print(f"[A4] stop（运行中 {log['running_at_stop2']}）", flush=True)
        log["stop2_status"] = wait(c, tid, {"paused", "succeeded", "failed", "cancelled"}, 300)
        if log["stop2_status"] == "paused":
            ctl(c, tid, "finish")
            print("[A4] finish", flush=True)
            log["final"] = wait(c, tid, {"succeeded", "failed", "cancelled"})
        else:
            log["final"] = log["stop2_status"]
        ins = inspect(c, tid)
        cont = log["continue_at"]
        log["new_calls_after_continue"] = {
            s: sum(1 for x in ins["calls"] if x.get("subrun_id") == s and ts(x["created_at"]) > cont)
            for s in log["done_at_stop1"]
        }
        log["calls_final"] = {s["subrun_id"]: s["calls"] for s in ins["subruns"]}
        rec = collect(c, a.out, "a34", tid, log)
        print(f"[A3/A4] {log['final']} partial {rec['partial']} subruns "
              f"{[(s['id'], s['status'], s['cancel_reason']) for s in rec['subruns']]} "
              f"completed 之后新调用 {log['new_calls_after_continue']} tools {rec['tool_calls_used']}/"
              f"{rec['tool_call_limit']} located {rec['located']}/{rec['cited_n']}", flush=True)
    if a.only == "finish":
        sid, tid = send(c, Q_FINISH, "验收 立即写报告")
        print(f"[A4] turn {tid[-6:]}", flush=True)
        log = {"session_id": sid}
        while True:
            if (ans := answer_if_asked(c, tid)) is not None:
                log["answered"] = ans
                print(f"[A4] 回答提问 {ans}", flush=True)
            subs = inspect(c, tid)["subruns"]
            done = [s["subrun_id"] for s in subs if s["status"] == "completed"]
            running = [s["subrun_id"] for s in subs if s["status"] == "started"]
            early = [s["subrun_id"] for s in subs if s["status"] == "started" and s["calls"] <= 9]
            if done and early:
                break
            if task(c, tid)["status"] in ("succeeded", "failed", "cancelled"):
                raise RuntimeError("研究在停止前已结束")
            time.sleep(1)
        log["done_at_stop"], log["running_at_stop"] = done, running
        log["calls_at_stop"] = {s["subrun_id"]: s["calls"] for s in subs}
        ctl(c, tid, "stop")
        print(f"[A4] stop（已完成 {done}，运行中 {running}）", flush=True)
        log["stop_status"] = wait(c, tid, {"paused", "succeeded", "failed", "cancelled"}, 300)
        log["at_pause"] = {s["subrun_id"]: s["status"] for s in inspect(c, tid)["subruns"]}
        ctl(c, tid, "finish")
        print(f"[A4] finish（暂停时 {log['at_pause']}）", flush=True)
        log["final"] = wait(c, tid, {"succeeded", "failed", "cancelled"})
        rec = collect(c, a.out, a.tag, tid, log)
        print(f"[A4] {log['final']} partial {rec['partial']} subruns "
              f"{[(s['id'], s['status'], s['cancel_reason']) for s in rec['subruns']]} tools "
              f"{rec['tool_calls_used']}/{rec['tool_call_limit']} located {rec['located']}/{rec['cited_n']}",
              flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())

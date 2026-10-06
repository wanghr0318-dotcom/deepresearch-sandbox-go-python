#!/usr/bin/env python3
"""串并行对比的驱动（M4 Plan 14 Task 12，规格 §16.6）。

在服务器上以 root 运行（只读取 api.token 文件与测试用户的登录文件，不打印其内容）：

    sudo python3 run.py --creds /path/cred.json --out runs --limit 4

对 questions.json 的前 --limit 个问题，各以 serial 与 parallel 运行一次（顺序交替以抵消时段差异）：
测试用户新建一个专用会话 → 运维 POST /tasks {session_id, spec{text, deep_research, research{scheduling,
fixed_plan}}}（运维路径 C12-7）→ 轮询到 turn 终态 → 导出 inspect、任务事件、报告与报告引用的结果 blob
→ 删除会话。每次运行写 <out>/<qid>-<mode>.json 与 <out>/<qid>-<mode>/（report.md、blobs/<sha>）。
只用标准库。
"""

from __future__ import annotations

import argparse
import http.cookiejar
import json
import re
import secrets
import ssl
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path

HERE = Path(__file__).resolve().parent
TERMINAL = {"succeeded", "failed", "cancelled"}
STUCK = {"paused", "awaiting_input"}
CITE = re.compile(r"\[(\d+)\]")
EVIDENCE = re.compile(r"^- \[(\d+)\] .* — (\S+) — sha256:([0-9a-f]{64})\s*$")


class Client:
    """经 127.0.0.1:443 访问本机服务（Host 与 Origin 为对外地址；自签证书不校验）。"""

    def __init__(self, host: str, token_file: Path) -> None:
        self.host = host
        self.base = "https://127.0.0.1"
        self.token = token_file.read_text().strip()
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        self.jar = http.cookiejar.CookieJar()
        self.opener = urllib.request.build_opener(
            urllib.request.HTTPSHandler(context=ctx), urllib.request.HTTPCookieProcessor(self.jar)
        )

    def call(
        self,
        method: str,
        path: str,
        body: object | None = None,
        *,
        admin: bool = False,
        accept: str | None = None,
        timeout: float = 60,
    ) -> tuple[int, bytes]:
        data = None if body is None else json.dumps(body, ensure_ascii=False).encode()
        req = urllib.request.Request(self.base + path, data=data, method=method)
        req.add_header("Host", self.host)
        req.add_header("Origin", "https://" + self.host)
        if data is not None:
            req.add_header("Content-Type", "application/json")
        if accept:
            req.add_header("Accept", accept)
        if admin:
            req.add_header("Authorization", "Bearer " + self.token)
        try:
            with self.opener.open(req, timeout=timeout) as resp:
                return resp.status, resp.read()
        except urllib.error.HTTPError as e:
            return e.code, e.read()

    def json(self, method: str, path: str, body: object | None = None, *, admin: bool = False,
             expect: tuple[int, ...] = (200,)) -> dict:
        st, b = self.call(method, path, body, admin=admin)
        if st not in expect:
            raise RuntimeError(f"{method} {path} → {st}: {b[:300]!r}")
        return json.loads(b) if b else {}

    def sse(self, path: str, idle: float = 8.0) -> list[dict]:
        """读取任务事件流直到连接关闭或 idle 秒无数据（任务已终态时全部历史事件立即发出）。"""
        req = urllib.request.Request(self.base + path, method="GET")
        req.add_header("Host", self.host)
        req.add_header("Accept", "text/event-stream")
        req.add_header("Authorization", "Bearer " + self.token)
        out: list[dict] = []
        try:
            with self.opener.open(req, timeout=idle) as resp:
                data: list[str] = []
                while True:
                    try:
                        raw = resp.readline()
                    except TimeoutError:
                        break
                    if not raw:
                        break
                    line = raw.decode("utf-8").rstrip("\n")
                    if line.startswith("data:"):
                        data.append(line[5:].strip())
                    elif line == "" and data:
                        try:
                            out.append(json.loads("\n".join(data)))
                        except json.JSONDecodeError:
                            pass
                        data = []
        except (TimeoutError, urllib.error.URLError):
            pass
        return out


def rid() -> str:
    return "cmp-" + secrets.token_hex(8)


def ts(s: str) -> float:
    return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()


def schedule(questions: list[dict], limit: int) -> list[tuple[dict, str]]:
    runs = []
    for i, q in enumerate(questions[:limit]):
        modes = ["serial", "parallel"] if i % 2 == 0 else ["parallel", "serial"]
        runs += [(q, m) for m in modes]
    return runs


def run_one(c: Client, q: dict, mode: str, out: Path, timeout_s: float) -> dict:
    tag = f"{q['id']}-{mode}"
    sess = c.json("POST", "/sessions", {"request_id": rid(), "title": f"subrun-compare {tag}"},
                  expect=(201,))
    sid = sess["session_id"]
    spec = {"text": q["text"], "deep_research": True,
            "research": {"scheduling": mode, "fixed_plan": q["fixed_plan"]}}
    t0 = time.time()
    task_id = c.json("POST", "/tasks", {"request_id": rid(), "session_id": sid, "spec": spec},
                     admin=True, expect=(201,))["task_id"]
    print(f"[{tag}] session {sid[-6:]} turn {task_id[-6:]} 已创建", flush=True)
    status, stuck = "", None
    while time.time() - t0 < timeout_s:
        task = c.json("GET", f"/tasks/{task_id}", admin=True)
        status = task["status"]
        if status in TERMINAL:
            break
        if status in STUCK:
            stuck = status
            c.json("POST", f"/tasks/{task_id}/cancel", {"request_id": rid(), "reason": "experiment"},
                   admin=True)
        time.sleep(5)
    else:
        stuck = "timeout"
        c.json("POST", f"/tasks/{task_id}/cancel", {"request_id": rid(), "reason": "experiment timeout"},
               admin=True)
        for _ in range(60):
            if c.json("GET", f"/tasks/{task_id}", admin=True)["status"] in TERMINAL:
                break
            time.sleep(5)
    polled_s = time.time() - t0
    rundir = out / tag
    (rundir / "blobs").mkdir(parents=True, exist_ok=True)
    inspect = c.json("GET", f"/tasks/{task_id}/inspect", admin=True)
    events = c.sse(f"/tasks/{task_id}/events")
    (rundir / "inspect.json").write_text(json.dumps(inspect, ensure_ascii=False, indent=1))
    (rundir / "events.json").write_text(json.dumps(events, ensure_ascii=False, indent=1))
    report = ""
    st, b = c.call("GET", f"/tasks/{task_id}/artifacts/report", admin=True)
    if st == 200:
        report = b.decode("utf-8")
        (rundir / "report.md").write_text(report)
    evidence = {}
    for line in report.split("\n"):
        if m := EVIDENCE.match(line):
            evidence[int(m.group(1))] = {"url": m.group(2), "sha256": m.group(3)}
    located = {}
    for n, ev in evidence.items():
        st, b = c.call("GET", f"/turns/{task_id}/raw/{ev['sha256']}", admin=True)
        located[n] = st == 200
        if st == 200:
            (rundir / "blobs" / ev["sha256"]).write_bytes(b)
    c.call("DELETE", f"/sessions/{sid}")
    rec = metrics(q, mode, task_id, inspect, events, report, evidence, located)
    rec.update({"session_id": sid, "stuck": stuck, "polled_s": round(polled_s, 1)})
    (out / f"{tag}.json").write_text(json.dumps(rec, ensure_ascii=False, indent=1))
    return rec


def metrics(q: dict, mode: str, task_id: str, inspect: dict, events: list[dict], report: str,
            evidence: dict, located: dict) -> dict:
    task = inspect["task"]
    created = ts(task["created_at"])
    ready = None
    partial = None
    for e in events:
        p = e.get("payload") or {}
        if p.get("kind") == "report_ready" or e.get("type") == "report_ready":
            ready = ts(e["ts"])
            partial = (p.get("data") or {}).get("partial", p.get("partial"))
    end = ready or max((ts(e["ts"]) for e in events), default=created)
    subs = []
    for s in inspect.get("subruns") or []:
        a = ts(s["started_at"])
        z = ts(s["ended_at"]) if s.get("ended_at") else None
        subs.append({"id": s["subrun_id"], "status": s["status"], "start_s": round(a - created, 1),
                     "dur_s": round(z - a, 1) if z else None, "calls": s["calls"],
                     "spent_micro": s["spent_micro"], "unknown_micro": s["unknown_micro"],
                     "cap_micro": s.get("cap_micro"), "failure_reason": s.get("failure_reason"),
                     "cancel_reason": s.get("cancel_reason")})
    spans = [(ts(s["started_at"]), ts(s["ended_at"])) for s in inspect.get("subruns") or [] if s.get("ended_at")]
    research_span = (max(z for _, z in spans) - min(a for a, _ in spans)) if spans else None
    busy = sum(z - a for a, z in spans)
    calls = inspect.get("calls") or []
    model_calls = [c for c in calls if "chat" in c["endpoint"]]
    by_model: dict[str, int] = {}
    for c in model_calls:
        by_model[c.get("model", "?")] = by_model.get(c.get("model", "?"), 0) + 1
    budget = inspect.get("budget") or {}
    body = report.split("\n## 证据", 1)[0]
    cited = sorted({int(n) for n in CITE.findall(body)})
    return {
        "question": q["id"], "lang": q["lang"], "mode": mode, "task_id": task_id,
        "status": task["status"], "status_reason": task.get("status_reason"),
        "attempts": task.get("attempts_total"),
        "wall_s": round(end - created, 1), "report_ready": ready is not None, "partial": partial,
        "research_span_s": round(research_span, 1) if research_span else None,
        "subrun_busy_s": round(busy, 1), "overlap": round(busy / research_span, 2) if research_span else None,
        "subruns": subs,
        "model_calls": len(model_calls), "model_calls_by_model": by_model,
        "tool_calls": sum(1 for c in calls if c["endpoint"].endswith(("/search", "/fetch"))),
        "tool_calls_used": budget.get("tool_calls_used"), "tool_call_limit": budget.get("tool_call_limit"),
        "failed_calls": sum(1 for c in calls if c["state"] in ("failed", "unknown")),
        "spent_micro": budget.get("spent_micro"), "unknown_micro": budget.get("unknown_micro"),
        "report_chars": len(body), "cited": cited, "evidence_n": len(evidence),
        "located": sum(1 for n in cited if located.get(n)), "cited_n": len(cited),
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--creds", type=Path, required=True, help="测试用户登录 JSON（username、password）")
    ap.add_argument("--token-file", type=Path, default=Path("/var/lib/agentbox/api.token"))
    ap.add_argument("--host", default="49.235.41.14")
    ap.add_argument("--questions", type=Path, default=HERE / "questions.json")
    ap.add_argument("--out", type=Path, default=HERE / "runs")
    ap.add_argument("--limit", type=int, default=10)
    ap.add_argument("--max-usd", type=float, default=4.0, help="累计费用（按配置单价）达到上限前停止新运行")
    ap.add_argument("--timeout-min", type=float, default=30)
    args = ap.parse_args()
    questions = json.loads(args.questions.read_text())["questions"]
    c = Client(args.host, args.token_file)
    c.json("POST", "/auth/login", json.loads(args.creds.read_text()))
    args.out.mkdir(parents=True, exist_ok=True)
    spent, worst = 0.0, 0.0
    for q, mode in schedule(questions, args.limit):
        tag = f"{q['id']}-{mode}"
        if (args.out / f"{tag}.json").exists():
            rec = json.loads((args.out / f"{tag}.json").read_text())
        else:
            if spent + worst > args.max_usd:
                print(f"[{tag}] 跳过：累计 {spent:.2f} USD + 单次最高 {worst:.2f} > {args.max_usd}", flush=True)
                break
            rec = run_one(c, q, mode, args.out, args.timeout_min * 60)
        usd = ((rec.get("spent_micro") or 0) + (rec.get("unknown_micro") or 0)) / 1e6
        spent, worst = spent + usd, max(worst, usd)
        print(f"[{tag}] {rec['status']} wall {rec['wall_s']} s，子主题 "
              f"{[s['status'] for s in rec['subruns']]}，工具 {rec['tool_calls_used']}，"
              f"{usd:.3f} USD（累计 {spent:.2f}）", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())

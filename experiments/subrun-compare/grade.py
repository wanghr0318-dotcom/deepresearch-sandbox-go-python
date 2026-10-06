#!/usr/bin/env python3
"""引用质量评分与汇总（M4 Plan 14 Task 12）。

在服务器上以 root 运行（读取 /etc/agentbox/agentbox.env 中的模型 Key，不打印）：

    sudo python3 grade.py --runs runs --pairs 8

对每份报告：
- 可定位率（自动，run.py 已记录）：正文中每个 [n] 在"## 证据"列表中，且其 sha256 是本 turn 已保存、
  经 GET /turns/{id}/raw/{sha} 可取到的结果 blob。
- 支持结论率（rubric）：按固定种子从正文中抽取至多 --pairs 个"句子 + 引用编号"对；证据文本为该 blob
  经 Worker 同一函数（page_text，截断到 6 KiB = 模型实际看到的正文）得到的文字；以 kimi-k3 按 RUBRIC
  判为 supported / partial / unsupported。支持结论率 = supported / 已判定对数（partial 单列）。
写 <runs>/grades.json（每对的判定与证据窗口，供人工复核）与 <runs>/summary.json。只用标准库与已安装的
agentbox_worker（/opt/agentbox）。
"""

from __future__ import annotations

import argparse
import json
import random
import re
import statistics
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

RUBRIC = """你是严格的事实核查员。给你报告中的一句话（"结论"）和它所引用的来源正文（"证据"，可能被截断）。
只根据证据判断结论是否被支持：
- supported：结论中的关键事实（数字、日期、主体、因果）都能在证据中找到，或可直接推出；
- partial：部分关键事实被支持，另有部分在证据中找不到或更具体/更强；
- unsupported：证据没有提到结论的关键事实，或与之矛盾。
不要使用你自己的知识补全证据。只输出一行 JSON：{"verdict": "supported|partial|unsupported", "reason": "≤60 字"}"""

CITE = re.compile(r"\[(\d+)\]")
SENT_END = re.compile(r"(?<=[。！？!?；])|(?<=\.)\s+|\n+")
SHOWN_MAX_BYTES = 6 * 1024
EVIDENCE_LINE = re.compile(r"^- \[(\d+)\] .* — (\S+) — sha256:([0-9a-f]{64})\s*$")


def env_value(path: Path, name: str) -> str:
    for line in path.read_text().splitlines():
        if line.startswith(name + "="):
            return line.split("=", 1)[1].strip().strip("'\"")
    raise SystemExit(f"{path} 中没有 {name}")


def claim_pairs(report: str) -> list[tuple[str, int]]:
    """正文（"## 证据"之前、去掉标题与引用块）中带 [n] 的句子 → (句子, n)。"""
    body = report.split("\n## 证据", 1)[0]
    lines = [ln for ln in body.split("\n") if not ln.startswith(("#", ">"))]
    pairs = []
    for sent in SENT_END.split("\n".join(lines)):
        sent = sent.strip(" -*|\t")
        if not sent:
            continue
        for n in dict.fromkeys(int(x) for x in CITE.findall(sent)):
            pairs.append((sent, n))
    return pairs


def evidence_text(blob: bytes) -> str:
    sys.path.insert(0, "/opt/agentbox")
    from agentbox_worker.tools.text import page_text, truncate_utf8  # noqa: PLC0415

    try:
        body = json.loads(blob)
    except ValueError:
        return ""
    if not isinstance(body, dict):
        return ""
    text = page_text(body) if body.get("encoding", "utf-8") == "utf-8" else ""
    return truncate_utf8(text, SHOWN_MAX_BYTES)


def judge(key: str, base: str, model: str, claim: str, evidence: str) -> tuple[dict, dict]:
    body = {
        "model": model,
        "messages": [
            {"role": "system", "content": RUBRIC},
            {"role": "user", "content": f"结论：{claim}\n\n证据：\n{evidence}"},
        ],
        "max_tokens": 4096,
    }
    req = urllib.request.Request(base.rstrip("/") + "/chat/completions",
                                 data=json.dumps(body, ensure_ascii=False).encode(), method="POST")
    req.add_header("Content-Type", "application/json")
    req.add_header("Authorization", "Bearer " + key)
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=180) as resp:
                out = json.loads(resp.read())
            break
        except (urllib.error.URLError, TimeoutError) as e:
            if attempt == 2:
                return {"verdict": "error", "reason": type(e).__name__}, {}
            time.sleep(5 * (attempt + 1))
    text = out["choices"][0]["message"].get("content") or ""
    m = re.search(r"\{.*\}", text, re.S)
    try:
        verdict = json.loads(m.group(0)) if m else {}
    except ValueError:
        verdict = {}
    if verdict.get("verdict") not in ("supported", "partial", "unsupported"):
        verdict = {"verdict": "error", "reason": text[:120]}
    return verdict, out.get("usage") or {}


def pct(xs: list[float], p: float) -> float | None:
    if not xs:
        return None
    xs = sorted(xs)
    k = (len(xs) - 1) * p
    lo, hi = int(k), min(int(k) + 1, len(xs) - 1)
    return round(xs[lo] + (xs[hi] - xs[lo]) * (k - lo), 1)


def summarize(runs: list[dict], grades: dict) -> dict:
    out = {}
    for mode in ("serial", "parallel"):
        rs = [r for r in runs if r["mode"] == mode]
        ok = [r for r in rs if r["status"] == "succeeded" and r["report_ready"]]
        g = [p for r in rs for p in grades.get(r["task_id"], []) if p["verdict"] != "error"]
        usd = [((r["spent_micro"] or 0) + (r["unknown_micro"] or 0)) / 1e6 for r in rs]
        out[mode] = {
            "runs": len(rs),
            "failed": len(rs) - len(ok),
            "partial": sum(1 for r in rs if r.get("partial")),
            "subruns_not_completed": sum(1 for r in rs for s in r["subruns"] if s["status"] != "completed"),
            "wall_p50_s": pct([r["wall_s"] for r in ok], 0.5),
            "wall_p95_s": pct([r["wall_s"] for r in ok], 0.95),
            "research_span_p50_s": pct([r["research_span_s"] for r in ok if r["research_span_s"]], 0.5),
            "overlap_mean": round(statistics.mean(r["overlap"] for r in ok if r["overlap"]), 2) if ok else None,
            "usd_median": round(statistics.median(usd), 3) if usd else None,
            "usd_total": round(sum(usd), 3),
            "unknown_micro_total": sum(r["unknown_micro"] or 0 for r in rs),
            "model_calls_median": statistics.median(r["model_calls"] for r in rs) if rs else None,
            "tool_calls_median": statistics.median(r["tool_calls_used"] or 0 for r in rs) if rs else None,
            "report_chars_median": statistics.median(r["report_chars"] for r in ok) if ok else None,
            "cited_median": statistics.median(r["cited_n"] for r in ok) if ok else None,
            "locatable_rate": round(sum(r["located"] for r in ok) / max(1, sum(r["cited_n"] for r in ok)), 3),
            "pairs_judged": len(g),
            "supported_rate": round(sum(p["verdict"] == "supported" for p in g) / len(g), 3) if g else None,
            "partial_rate": round(sum(p["verdict"] == "partial" for p in g) / len(g), 3) if g else None,
            "unsupported_rate": round(sum(p["verdict"] == "unsupported" for p in g) / len(g), 3) if g else None,
        }
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", type=Path, default=Path(__file__).resolve().parent / "runs")
    ap.add_argument("--pairs", type=int, default=8)
    ap.add_argument("--seed", type=int, default=20261006)
    ap.add_argument("--env", type=Path, default=Path("/etc/agentbox/agentbox.env"))
    ap.add_argument("--base-url", default="https://api.moonshot.cn/v1")
    ap.add_argument("--model", default="kimi-k3")
    ap.add_argument("--in-price", type=float, default=3.0, help="USD / 百万输入 token（与服务端配置相同）")
    ap.add_argument("--out-price", type=float, default=15.0)
    ap.add_argument("--summary-only", action="store_true")
    args = ap.parse_args()
    runs = [json.loads(p.read_text()) for p in sorted(args.runs.glob("q*-*.json"))]
    gpath = args.runs / "grades.json"
    grades = json.loads(gpath.read_text()) if gpath.exists() else {}
    spath = args.runs / "summary.json"
    prev = json.loads(spath.read_text()).get("grading_usage", {}) if spath.exists() else {}
    usage_in, usage_out = prev.get("prompt_tokens", 0), prev.get("completion_tokens", 0)
    if not args.summary_only:
        key = env_value(args.env, "AGENTBOX_MODEL_API_KEY")
        for r in runs:
            if r["task_id"] in grades:
                continue
            rundir = args.runs / f"{r['question']}-{r['mode']}"
            report_path = rundir / "report.md"
            if not report_path.exists():
                continue
            report = report_path.read_text()
            shas = {int(m.group(1)): m.group(3) for ln in report.split("\n") if (m := EVIDENCE_LINE.match(ln))}
            pairs = claim_pairs(report)
            rng = random.Random(f"{args.seed}-{r['question']}-{r['mode']}")
            sample = rng.sample(pairs, min(args.pairs, len(pairs)))
            judged = []
            for claim, n in sample:
                sha = shas.get(n)
                blob = rundir / "blobs" / sha if sha else None
                ev = evidence_text(blob.read_bytes()) if blob and blob.exists() else ""
                if not ev:
                    judged.append({"claim": claim, "n": n, "sha256": sha, "verdict": "no_text", "evidence": ""})
                    continue
                v, usage = judge(key, args.base_url, args.model, claim, ev)
                usage_in += usage.get("prompt_tokens", 0)
                usage_out += usage.get("completion_tokens", 0)
                judged.append({"claim": claim, "n": n, "sha256": sha, **v, "evidence": ev})
            grades[r["task_id"]] = judged
            gpath.write_text(json.dumps(grades, ensure_ascii=False, indent=1))
            print(f"{r['question']}-{r['mode']}: " + ", ".join(p["verdict"] for p in judged), flush=True)
    summary = summarize(runs, {k: [p for p in v if p["verdict"] != "no_text"] for k, v in grades.items()})
    summary["grading_usage"] = {"prompt_tokens": usage_in, "completion_tokens": usage_out,
                                "usd": round(usage_in * args.in_price / 1e6 + usage_out * args.out_price / 1e6, 3)}
    (args.runs / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=1))
    print(json.dumps(summary, ensure_ascii=False, indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main())

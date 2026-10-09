#!/usr/bin/env python3
"""Print a Tempo trace (GET /api/traces/<id>, OTLP JSON) as an indented span tree with durations.

Usage: python3 scripts/dev/span-tree.py trace.json
Only span names, durations, status and a few allowlisted attributes are printed.
"""

from __future__ import annotations

import base64
import json
import sys

SHOW = (
    "http.route", "http.status_code", "task.kind", "task.status", "attempt.no", "outcome.class",
    "worker.mode", "gateway.kind", "gateway.model", "call.result", "try.no", "try.outcome",
    "checkpoint.status", "cost.actual_micro", "kill.reason", "control", "desired",
)


def hexid(v: str) -> str:
    if not v:
        return ""
    if len(v) in (16, 32) and all(c in "0123456789abcdef" for c in v):
        return v
    return base64.b64decode(v).hex()


def attr_value(v: dict) -> object:
    for k in ("stringValue", "intValue", "boolValue", "doubleValue"):
        if k in v:
            return v[k]
    return None


def main() -> None:
    doc = json.load(open(sys.argv[1], encoding="utf-8"))
    spans = []
    for rs in doc.get("batches") or doc.get("resourceSpans") or []:
        for ss in rs.get("scopeSpans") or rs.get("instrumentationLibrarySpans") or []:
            spans.extend(ss.get("spans", []))
    by_id = {hexid(s["spanId"]): s for s in spans}
    children: dict[str, list] = {}
    roots = []
    for s in spans:
        p = hexid(s.get("parentSpanId", ""))
        if p and p in by_id:
            children.setdefault(p, []).append(s)
        else:
            roots.append(s)

    def start(s: dict) -> int:
        return int(s["startTimeUnixNano"])

    def show(s: dict, depth: int) -> None:
        ms = (int(s["endTimeUnixNano"]) - start(s)) / 1e6
        attrs = {a["key"]: attr_value(a["value"]) for a in s.get("attributes", [])}
        extra = " ".join(f"{k}={attrs[k]}" for k in SHOW if k in attrs)
        err = s.get("status", {}).get("code") in ("STATUS_CODE_ERROR", 2)
        mark = f" ERROR({s['status'].get('message', '')})" if err else ""
        print(f"{'  ' * depth}{s['name']}  {ms:.1f} ms  {extra}{mark}".rstrip())
        for c in sorted(children.get(hexid(s["spanId"]), []), key=start):
            show(c, depth + 1)

    print(f"{len(spans)} spans")
    for r in sorted(roots, key=start):
        show(r, 0)


if __name__ == "__main__":
    main()

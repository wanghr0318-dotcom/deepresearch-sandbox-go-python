"""Measure single-request token limits on the pinned SGLang service."""
import argparse
import json
from pathlib import Path
from time import monotonic
from urllib.error import HTTPError
from urllib.request import Request, urlopen


def measure(base_url, prompt_tokens, output_budget):
    # Fixed token ids keep the requested input length independent of chat templates.
    payload = {"model": "Qwen/Qwen3-8B", "prompt": [42] * prompt_tokens,
               "temperature": 0, "max_tokens": output_budget}
    request = Request(base_url.rstrip("/") + "/completions",
                      data=json.dumps(payload).encode(), headers={"Content-Type": "application/json"})
    start = monotonic()
    row = {"requested_input": prompt_tokens, "output_budget": output_budget}
    try:
        with urlopen(request, timeout=120) as response:
            body = json.load(response)
            row.update(http=response.status, model=body["model"], usage=body["usage"],
                       finish_reason=body["choices"][0]["finish_reason"])
    except HTTPError as err:
        row.update(http=err.code, error=err.read(4096).decode(errors="replace"))
    row["elapsed_ms"] = round((monotonic() - start) * 1000)
    return row


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("base_url", help="SGLang base URL ending in /v1")
    parser.add_argument("report", type=Path)
    parser.add_argument("--inputs", nargs="+", type=int, default=[1024, 8192, 16384, 24512, 24577])
    parser.add_argument("--output-budget", type=int, default=64)
    args = parser.parse_args()
    with args.report.open("x", encoding="utf-8") as report:
        for length in args.inputs:
            row = measure(args.base_url, length, args.output_budget)
            print(json.dumps(row, ensure_ascii=False), file=report, flush=True)
            print(length, row["http"], flush=True)
            if row["http"] >= 500:
                raise SystemExit(1)

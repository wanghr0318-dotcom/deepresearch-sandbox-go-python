"""Replay the same two tool-call turns against SGLang and the Go Gateway."""

import argparse
import json
import os
from pathlib import Path
from time import perf_counter
from urllib.request import Request, urlopen


def chat(endpoint, token, messages):
    payload = {
        "model": "Qwen/Qwen3-8B",
        "messages": messages,
        "temperature": 0,
        "max_tokens": 512,
        "tools": [{
            "type": "function",
            "function": {
                "name": "read_file",
                "description": "Read a workspace file.",
                "parameters": {
                    "type": "object",
                    "properties": {"path": {"type": "string"}},
                    "required": ["path"],
                },
            },
        }],
    }
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = Request(endpoint, data=json.dumps(payload).encode(), headers=headers)
    start = perf_counter()
    with urlopen(request, timeout=120) as response:
        body = json.load(response)
    return {"request": payload, "response": body, "elapsed_ms": round((perf_counter() - start) * 1000)}


def message(exchange):
    response = exchange["response"]
    if response.get("model") != "Qwen/Qwen3-8B":
        raise ValueError(f"unexpected response model: {response.get('model')}")
    return response["choices"][0]["message"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("base_url", help="endpoint root ending in /v1")
    parser.add_argument("report", type=Path)
    args = parser.parse_args()
    endpoint = args.base_url.rstrip("/") + "/chat/completions"
    token = os.environ.get("AGENTBOX_PROBE_TOKEN")
    messages = [{
        "role": "user",
        "content": "Call read_file with path probe.txt. Copy the returned string verbatim as your entire final answer. No introduction, quotes, Markdown, or code fences.",
    }]
    exchanges = []
    try:
        exchanges.append(chat(endpoint, token, messages))
        (call,) = message(exchanges[0])["tool_calls"]
        function = call["function"]
        if function["name"] != "read_file" or json.loads(function["arguments"]) != {"path": "probe.txt"}:
            raise ValueError("unexpected read_file arguments")
        fixture = "agentbox-stage2-proof-47c3"
        messages = [
            *messages,
            {"role": "assistant", "content": "", "tool_calls": [call]},
            {"role": "tool", "tool_call_id": call["id"], "content": fixture},
        ]
        exchanges.append(chat(endpoint, token, messages))
        reply = message(exchanges[1])
        if (reply.get("content") or "").strip() != fixture or reply.get("tool_calls"):
            raise ValueError("model did not consume the second-round tool result")
    finally:
        args.report.write_text(json.dumps(exchanges, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()

"""Replay user turns through the session API and archive their observable events."""
import argparse
import json
import os
from pathlib import Path
import time
from urllib.request import Request, urlopen
from uuid import uuid4


def replay(base, token, scenario, out, timeout):
    def request(path, method="GET", body=None, headers=None):
        return Request(base.rstrip("/") + path, method=method,
                       data=None if body is None else json.dumps(body).encode(),
                       headers={"Authorization": "Bearer " + token,
                                "Content-Type": "application/json", **(headers or {})})

    def api(path, method, body=None):
        with urlopen(request(path, method, body), timeout=15) as response:
            return json.load(response)

    directory = out / scenario["id"]
    directory.mkdir()
    session = api("/sessions", "POST", {"request_id": uuid4().hex,
                                       "title": scenario["id"]})["session_id"]
    path = "/sessions/" + session
    cursor = "0"
    try:
        for index, turn in enumerate(scenario["turns"], 1):
            start = time.monotonic()
            task = api(path + "/messages", "POST", {"request_id": f"turn-{index}",
                       "text": turn["text"], "deep_research": True})["turn_id"]
            with (directory / f"turn-{index}.jsonl").open("x", encoding="utf8") as log:
                def save(row):
                    log.write(json.dumps(row, ensure_ascii=False) + "\n")
                    log.flush()

                save({"session_id": session, "turn_id": task, "input": turn["text"]})
                with urlopen(request(path + "/events", headers={"Last-Event-ID": cursor}),
                             timeout=timeout) as stream:
                    for raw in stream:
                        if time.monotonic() - start >= timeout:
                            raise TimeoutError(f"{task} exceeded {timeout}s")
                        if not raw.startswith(b"data:"):
                            continue
                        event = json.loads(raw[5:])
                        cursor = str(event["seq"])
                        if event.get("turn_id") != task:
                            continue
                        save(event)
                        if event["type"] == "turn_status":
                            status = event["data"]["status"]
                            if status == "succeeded":
                                break
                            if status in {"failed", "cancelled", "paused", "awaiting_input"}:
                                raise RuntimeError(f"{task}: {status}")
                    else:
                        raise EOFError(f"stream ended before {task} finished")
                save({"elapsed_s": round(time.monotonic() - start, 3)})
    finally:
        api(path, "DELETE")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", required=True)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--timeout", type=int, default=900)
    args = parser.parse_args()
    token = os.environ["AGENTBOX_RESEARCH_TOKEN"]
    cases = json.loads(Path(__file__).with_name("research-dialogues.json").read_bytes())
    args.out.mkdir(parents=True, exist_ok=False)
    for case in cases:
        replay(args.base, token, case, args.out, args.timeout)


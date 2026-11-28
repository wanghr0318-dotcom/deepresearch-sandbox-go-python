"""One-round B witness for the isolated phase-four test adapter."""

import argparse
import datetime
import json
import os
import pathlib
import socket
import time
import urllib.error
import urllib.request
import uuid


OPERATIONS = ("renew", "checkpoint", "file_head", "final_result")
OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", type=pathlib.Path, default=pathlib.Path("/var/tmp/abx-reconnect-20261128"),
                        help="Private directory containing token and operator cut.flag")
    parser.add_argument("--port", type=int, default=19489, help="Local loopback port forwarded to the test adapter")
    args = parser.parse_args()
    if not 1 <= args.port <= 65535:
        parser.error("--port must be in the range 1..65535")
    endpoint = f"http://127.0.0.1:{args.port}"
    token = (args.state_dir / "token").read_text().strip()
    if not token:
        raise ValueError("test binding missing")
    pid = os.getpid()
    ticks = pathlib.Path("/proc/self/stat").read_text().rsplit(")", 1)[1].split()[19]
    boot = pathlib.Path("/proc/sys/kernel/random/boot_id").read_text().strip()
    started = time.monotonic()
    run = "trial-" + uuid.uuid4().hex
    old = ""

    def record(event, **fields):
        row = dict(timestamp=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                   monotonic_s=time.monotonic(), pid=pid, start_ticks=ticks,
                   boot_id=boot, b_host=socket.gethostname(), event=event, **fields)
        print(json.dumps(row, separators=(",", ":")), flush=True)

    def request(path, op_id=None):
        headers = {"Authorization": "Bearer " + token}
        if old:
            headers["X-Attempt-ID"] = old
        if op_id:
            headers["X-Operation-ID"] = op_id
        req = urllib.request.Request(endpoint + path, headers=headers,
                                     method="POST" if op_id else "GET",
                                     data=b"" if op_id else None)
        try:
            response = OPENER.open(req, timeout=12)
        except urllib.error.HTTPError as exc:
            response = exc
        except (OSError, urllib.error.URLError) as exc:
            return None, {}, type(getattr(exc, "reason", exc)).__name__
        with response:
            raw = response.read(4096)
            try:
                body = json.loads(raw) if raw else {}
            except (json.JSONDecodeError, UnicodeDecodeError):
                body = {}
            return response.status, body if isinstance(body, dict) else {}, None

    def write(stage):
        for op in OPERATIONS:
            op_id = f"{run}-{stage}-{op}"
            record("request_start", stage=stage, op=op, operation_id=op_id, old_attempt=old)
            status, body, error = request("/write/" + op, op_id)
            record("request_end", stage=stage, op=op, operation_id=op_id,
                   status=status, outcome="connection_failure" if error else "http_response",
                   error=error, layer=body.get("layer"), code=body.get("code"),
                   accepted=body.get("accepted"), old_attempt=old)

    record("start")
    isolated = False
    failures = 0
    last_beat = 0
    while time.monotonic() - started < 330:
        now = time.monotonic()
        if now - last_beat >= 1:
            record("heartbeat", old_attempt=old)
            last_beat = now
        status, state, error = request("/state")
        if status == 200:
            failures = 0
            if not old and state.get("phase") in ("prepared", "old") and state.get("old_attempt"):
                old = state["old_attempt"]
                record("bound", old_attempt=old)
            elif old and state.get("phase") == "new":
                if not isolated or state.get("old_attempt") != old or not state.get("new_attempt") or state["new_attempt"] == old:
                    raise RuntimeError("successor evidence or link isolation missing")
                record("successor", old_attempt=old, new_attempt=state["new_attempt"])
                write("reconnected")
                record("complete", old_attempt=old)
                return
        elif old and error:
            failures += 1
            if (args.state_dir / "cut.flag").exists() and not isolated:
                isolated = True
                record("disconnected", old_attempt=old, error=error)
                write("isolated")
        elif status is not None:
            record("state_http_error", status=status)
        time.sleep(0.8)
    record("timeout", old_attempt=old)
    raise SystemExit(2)


if __name__ == "__main__":
    main()

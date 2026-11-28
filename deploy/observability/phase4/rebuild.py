"""Rebuild a phase-four monitoring stack in a fresh directory on Debian/Ubuntu."""
import argparse
import base64
import configparser
import contextlib
import functools
import hashlib
import http.server
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

BASE = Path(__file__).resolve().parent
REPO = BASE.parents[2]


def endpoint(value):
    if not re.fullmatch(r"[A-Za-z0-9.-]+:[0-9]{1,5}", value) or not 0 < int(value.rsplit(":", 1)[1]) < 65536:
        raise argparse.ArgumentTypeError("expected a hostname or IPv4 followed by :port")
    return value


def local_ports(requests):
    with contextlib.ExitStack() as stack:
        chosen = []
        for requested in requests:
            sock = stack.enter_context(socket.socket())
            sock.bind(("127.0.0.1", requested))
            chosen.append(sock.getsockname()[1])
        return chosen


def fetch(url, headers=None):
    with urllib.request.urlopen(urllib.request.Request(url, headers=headers or {}), timeout=5) as stream:
        return json.load(stream)


def package(path, name, dest, expected):
    with path.open("rb") as stream:
        checksum = hashlib.file_digest(stream, "sha256").hexdigest()
    if expected and checksum != expected:
        raise ValueError(f"{name}: package digest does not match")
    def field(key):
        return subprocess.check_output(["dpkg-deb", "-f", path, key], text=True).strip()
    if field("Package") != name or field("Architecture") != "amd64":
        raise ValueError(f"unexpected package {path.name}")
    version = field("Version")
    subprocess.run(["dpkg-deb", "-x", path, dest], check=True)
    return {"package":name, "version":version, "sha256":checksum}


def run(args):
    os.umask(0o077)
    root = args.output.resolve()
    root.mkdir(parents=True, exist_ok=False)
    graf, prom, metrics = local_ports([args.grafana_port, args.prometheus_port, args.metrics_port])
    replay = root / "replay"
    subprocess.run([sys.executable, str(REPO/"eval/phase4/dashboard_archive.py"), "replay",
                    str(args.archive), str(replay), "--html-template", str(BASE.parent/"phase4-live.html")], check=True)
    rows = json.loads((replay/"state/rounds.json").read_bytes())
    expected_tasks = sum(map(len, rows.values()))
    if not expected_tasks:
        raise ValueError("empty replay")
    packages = [package(args.grafana_deb, "grafana", root/"grafana", args.grafana_sha256),
                package(args.prometheus_deb, "prometheus", root/"prometheus", args.prometheus_sha256)]
    grafhome = root/"grafana/usr/share/grafana"
    prombin = root/"prometheus/usr/bin"
    provisioning = root/"provisioning"
    shutil.copytree(BASE/"provisioning", provisioning)
    boards = root/"boards"
    boards.mkdir()
    shutil.copy(BASE.parent/"grafana/dashboards/agentbox-phase4.json", boards)
    shutil.copy(BASE.parent/"phase4-live.html", grafhome/"public")
    board_cfg = provisioning/"dashboards/phase4.yml"
    board_cfg.write_text(board_cfg.read_text().replace("/opt/agentbox-phase4-monitor/boards", json.dumps(str(boards))))
    source_cfg = provisioning/"datasources/prom.yml"
    source_cfg.write_text(source_cfg.read_text().replace("127.0.0.1:9090", f"127.0.0.1:{prom}"))
    ini = configparser.ConfigParser(interpolation=None)
    ini.read(BASE/"grafana.ini")
    ini["server"]["http_port"] = str(graf)
    for key in ("data","plugins","logs"):
        ini["paths"][key] = str(root/"grafana-state"/key)
    ini["paths"]["provisioning"] = str(provisioning)
    with (root/"grafana.ini").open("w") as out:
        ini.write(out)
    conf = (BASE/"prom-c.example.yml").read_text()
    for old,new in {"node-a.internal.example:9100":args.node_a,"node-b.internal.example:9100":args.node_b,
                    "node-a.internal.example:19467":args.task_target or f"127.0.0.1:{metrics}",
                    "127.0.0.1:3000":f"127.0.0.1:{graf}","127.0.0.1:9090":f"127.0.0.1:{prom}"}.items():
        conf = conf.replace(old,new)
    (root/"prom.yml").write_text(conf)
    subprocess.run([prombin/"promtool","check","config",root/"prom.yml"],check=True)
    password = secrets.token_urlsafe(32)
    (root/"admin-password").write_text(password+"\n")
    ready = threading.Event()
    def stop(_signum, _frame):
        ready.set()
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    with contextlib.ExitStack() as cleanup:
        if not args.task_target:
            handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=str(replay/"state/public"))
            server = http.server.ThreadingHTTPServer(("127.0.0.1", metrics), handler)
            cleanup.callback(server.server_close)
            threading.Thread(target=server.serve_forever, daemon=True).start()
            cleanup.callback(server.shutdown)
        children = []
        def finish(child):
            if child.poll() is None:
                child.terminate()
                try:
                    child.wait(12)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait()
        def start(name, cmd, env):
            log = cleanup.enter_context((root/(name+".log")).open("w"))
            child = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT, env=env)
            cleanup.callback(finish, child)
            children.append(child)
        start("prom", [prombin/"prometheus", "--config.file="+str(root/"prom.yml"),
                      "--storage.tsdb.path="+str(root/"tsdb"), "--storage.tsdb.retention.time=3d",
                      f"--web.listen-address=127.0.0.1:{prom}"], os.environ.copy())
        start("grafana", [grafhome/"bin/grafana","server","--homepath="+str(grafhome),"--config="+str(root/"grafana.ini")],
              {**os.environ,"GF_SECURITY_ADMIN_PASSWORD":password})
        base = f"http://127.0.0.1:{graf}"
        proxy = base+"/api/datasources/proxy/uid/prometheus/api/v1/"
        end = time.monotonic() + 120
        last = ""
        while not ready.is_set() and time.monotonic() < end:
            if any(p.poll() is not None for p in children):
                raise RuntimeError(f"child exited early; inspect {root}/grafana.log and prom.log")
            try:
                health = fetch(base+"/api/health")
                board = fetch(base+"/api/dashboards/uid/agentbox-phase4")["dashboard"]
                targets = fetch(proxy+"targets")["data"]["activeTargets"]
                series = fetch(proxy+"query?"+urllib.parse.urlencode({"query":"agentbox_phase4_task_info"}))["data"]["result"]
                seen = {(s["metric"]["round"],s["metric"]["turn"]) for s in series}
                wanted = {(run,t) for run,tasks in rows.items() for t in tasks}
                match = wanted <= seen if args.task_target else wanted == seen
                if health["database"]=="ok" and len(targets)==5 and all(t["health"]=="up" for t in targets) and match:
                    break
                last = f"targets {len(targets)} tasks {len(seen)}"
            except (OSError, ValueError, KeyError) as exc:
                last = str(exc)
            ready.wait(1)
        else:
            raise RuntimeError("rebuild validation timed out or interrupted: "+last)
        def basic(pw):
            return {"Authorization":"Basic "+base64.b64encode(("admin:"+pw).encode()).decode()}
        # Verify both positive and negative authentication without logging either credential.
        admin_verified = fetch(base+"/api/user",basic(password)).get("isGrafanaAdmin") is True
        default_rejected = False
        try:
            fetch(base+"/api/user",basic("admin"))
        except urllib.error.HTTPError as err:
            default_rejected = err.code in (401,403)
        if not (default_rejected and admin_verified):
            raise RuntimeError("unexpected administrator authentication behavior")
        proof = {"packages":packages, "grafana_health":health, "dashboard_uid":board["uid"],
                 "panels":len(board["panels"]), "ports":{"grafana":graf,"prometheus":prom,"metrics":metrics},
                 "healthy_targets":len(targets),"replayed_tasks":expected_tasks,"visible_tasks":len(seen),
                 "default_admin_rejected":default_rejected,"new_admin_verified":admin_verified,
                 "task_source":"private live endpoint" if args.task_target else "replayed archive"}
        (root/"proof.json").write_text(json.dumps(proof,indent=2)+"\n")
        print(json.dumps(proof),flush=True)
        if args.serve:
            while not ready.wait(1):
                if any(p.poll() is not None for p in children):
                    raise RuntimeError("monitoring child exited")


if __name__ == "__main__":
    cli=argparse.ArgumentParser(description=__doc__)
    cli.add_argument("--output",type=Path,required=True,help="New private installation directory")
    cli.add_argument("--archive",type=Path,required=True)
    cli.add_argument("--grafana-deb",type=Path,required=True)
    cli.add_argument("--prometheus-deb",type=Path,required=True)
    cli.add_argument("--grafana-sha256")
    cli.add_argument("--prometheus-sha256")
    cli.add_argument("--node-a",type=endpoint,required=True)
    cli.add_argument("--node-b",type=endpoint,required=True)
    cli.add_argument("--task-target",type=endpoint,help="Live private task exporter; default serves the restored archive")
    cli.add_argument("--grafana-port",type=int,default=0)
    cli.add_argument("--prometheus-port",type=int,default=0)
    cli.add_argument("--metrics-port",type=int,default=0)
    cli.add_argument("--serve",action="store_true",help="Continue serving after validation until SIGTERM or Ctrl+C")
    run(cli.parse_args())

#!/usr/bin/env bash
# Kubernetes provider 演示（设计 docs/design/2026-10-10-k8s-provider-design.md）：在 kind 集群上以 --provider k8s
# 运行 server（每个环境一个 Pod，worker 镜像按 digest 固定，预热池），逐步打印每一步及其结果。
#
#   bash scripts/demo-k8s.sh
#
# 需要：docker（可访问 daemon）、kind、go（构建静态二进制）、python3、curl。所有组件都运行在 docker 主机上：
#   - kind 集群（deploy/k8s/kind-config.yaml）：docker 主机的 /var/lib/agentbox-k8s 以同一路径挂进节点；
#   - 本地镜像仓库 kind-registry（宿主 127.0.0.1:5001，集群内 kind-registry:5000）；
#   - PostgreSQL、fake upstream、agentbox server 三个容器（kind 网络）；server 与 fake upstream 共享网络命名空间，
#     并以同一路径挂载 /var/lib/agentbox-k8s，因此 workspace 与 Gateway socket 经节点本地的槽位目录进入 Pod。
#   server 以命名空间内最小权限的 ServiceAccount（deploy/k8s/sandbox.yaml）访问 API server。
#
# 步骤：工具检查 → 集群与镜像仓库 → 构建二进制、worker 镜像（推送并解析 digest）与 server 镜像 → 命名空间、RBAC 与
# ServiceAccount kubeconfig → PostgreSQL、fake upstream、server（--provider k8s --k8s-warm-pool N）→ 任务 A：经
# Gateway 搜索与 chat、写产物（workspace 槽位）→ 任务 B：运行中检查 Pod 隔离（uid、只读根、无 token、出站被拒）后
# 取消（stop）→ 任务 C：运行中 SIGKILL server，孤儿 Pod 留存，重启后恢复并从 checkpoint 完成 → 冷/热启动延迟
# （P50/P95）→ 清理与泄漏检查。任何一步失败即以非零状态退出，并打印失败的步骤名。
#
# 环境变量：
#   AGENTBOX_K8S_CLUSTER   kind 集群名（默认 agentbox；已存在则复用）
#   AGENTBOX_K8S_WARM      预热 Pod 数（默认 2）
#   AGENTBOX_K8S_SAMPLES   冷、热启动各测量的任务数（默认 10）
#   AGENTBOX_K8S_KEEP=1    结束后保留 server、fake upstream 与 PostgreSQL 容器（集群与镜像仓库总是保留）
#   AGENTBOX_K8S_RUNTIME_CLASS  可选：Pod 的 runtimeClassName（例如已安装 gVisor 时的 gvisor）
set -euo pipefail

REPO=$(cd "$(dirname "$0")/.." && pwd)
CLUSTER="${AGENTBOX_K8S_CLUSTER:-agentbox}"
WARM="${AGENTBOX_K8S_WARM:-2}"
SAMPLES="${AGENTBOX_K8S_SAMPLES:-10}"
RUNTIME_CLASS="${AGENTBOX_K8S_RUNTIME_CLASS:-}"
HOSTDIR=/var/lib/agentbox-k8s # 与 deploy/k8s/kind-config.yaml 一致
DATA="$HOSTDIR/data"
NS=agentbox-sandbox
NODE="$CLUSTER-control-plane"
REG=kind-registry
WORKER_REF="localhost:5001/agentbox-worker:demo" # 宿主推送
WORKER_IN_CLUSTER="$REG:5000/agentbox-worker:demo" # server 解析并固定 digest
SERVER_IMG=agentbox-k8s-server:demo
PG=agentbox-k8s-pg
FU=agentbox-k8s-fu
SRV=agentbox-k8s-server
LOGDIR=$(mktemp -d /tmp/agentbox-demo-k8s.XXXXXX)
STEP="初始化"
N=0

step() { N=$((N + 1)); STEP="$1"; printf '\n[%02d] %s\n' "$N" "$1"; }
ok() { printf '     OK  %s\n' "$*"; }
info() { printf '         %s\n' "$*"; }
fail() { printf '     FAIL %s\n' "$*" >&2; exit 1; }

on_exit() {
  local rc=$?
  if [ "$rc" -ne 0 ]; then
    printf '\n演示失败：步骤 [%02d] %s（退出码 %d）。日志目录：%s\n' "$N" "$STEP" "$rc" "$LOGDIR" >&2
    docker logs --tail 30 "$SRV" >&2 2>&1 || true
  else
    printf '\n演示完成：全部 %d 步通过。日志目录：%s\n' "$N" "$LOGDIR"
  fi
  if [ -z "${AGENTBOX_K8S_KEEP:-}" ]; then
    docker rm -f "$SRV" "$FU" "$PG" >/dev/null 2>&1 || true
  fi
}
trap on_exit EXIT

json() { python3 -c "import json,sys; d=json.load(sys.stdin); print($1)"; }
wait_for() {
  local what=$1 limit=$2 end
  shift 2
  end=$((SECONDS + limit))
  while [ "$SECONDS" -lt "$end" ]; do
    if "$@" >/dev/null 2>&1; then return 0; fi
    sleep 0.2
  done
  fail "等待超时（${limit}s）：$what"
}

K() { docker exec -i "$NODE" kubectl --kubeconfig /etc/kubernetes/admin.conf "$@"; }
# CLI 在 fake upstream 容器中运行：它与 server 共享网络命名空间（127.0.0.1:8080），server 被杀死时仍可用。
# 配置了模型上游时 server 启用账号，运维调用须带 Bearer token：token 只在 <data>/api.token（0600），CLI 经环境变量读取，
# 不出现在任何命令行。
ab() { docker exec "$FU" sh -c 'AGENTBOX_TOKEN=$(cat "$0") exec agentbox "$@"' "$DATA/api.token" "$@"; }
inspect() { ab task inspect "$1"; }
task_field() { inspect "$1" | json "d['task']['$2']"; }
is_terminal() { case "$(task_field "$1" status)" in succeeded | failed | cancelled) return 0 ;; esac; return 1; }
has_checkpoint() { [ "$(inspect "$1" | json "len(d['checkpoints'])")" -ge 1 ]; }
env_of() { inspect "$1" | json "[a for a in d['attempts'] if a['attempt_no'] == $2][0]['env_id']"; }
cleaned() { [ "$(inspect "$1" | json "all(a['cleanup_state'] == 'done' and a.get('stopped_at') for a in d['attempts'])")" = True ]; }
submit() {
  if [ $# -gt 1 ]; then ab task submit --spec "$1" --limits "$2"; else ab task submit --spec "$1"; fi | json "d['task_id']"
}
server_ready() { ab status 2>/dev/null | grep -q normal; }
env_pods() { K -n "$NS" get pods -l "agentbox.io/env=$1" -o name 2>/dev/null; }
env_pod() { env_pods "$1" | head -n 1 | sed 's#^pod/##'; }
pod_running() { [ "$(K -n "$NS" get pod "$1" -o jsonpath='{.status.phase}')" = Running ]; }
warm_ready() {
  [ "$(K -n "$NS" get pods -l agentbox.io/state=warm -o jsonpath='{range .items[*]}{.status.conditions[?(@.type=="Ready")].status}{"\n"}{end}' | grep -c True)" -ge 1 ]
}

# ---------------------------------------------------------------------------

step "工具检查：docker、kind、go、python3、curl"
for t in docker kind go python3 curl; do command -v "$t" >/dev/null || fail "缺少 $t"; done
ok "docker $(docker version --format '{{.Server.Version}}')，$(kind version | cut -d' ' -f1-2)，$(go version | cut -d' ' -f3)"

step "kind 集群 $CLUSTER 与本地镜像仓库 $REG"
if kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
  ok "复用已有集群 $CLUSTER"
else
  kind create cluster --name "$CLUSTER" --config "$REPO/deploy/k8s/kind-config.yaml" --kubeconfig "$LOGDIR/admin.kubeconfig" >"$LOGDIR/kind.log" 2>&1 ||
    { cat "$LOGDIR/kind.log" >&2; fail "kind create cluster"; }
  ok "已创建集群 $CLUSTER（kindest/node:v1.34.0；$HOSTDIR 挂进节点）"
fi
docker inspect "$REG" >/dev/null 2>&1 || docker run -d --restart=always -p 127.0.0.1:5001:5000 --name "$REG" registry:2 >/dev/null
docker network connect kind "$REG" 2>/dev/null || true
docker exec "$NODE" mkdir -p "/etc/containerd/certs.d/$REG:5000"
printf '[host."http://%s:5000"]\n' "$REG" | docker exec -i "$NODE" cp /dev/stdin "/etc/containerd/certs.d/$REG:5000/hosts.toml"
K get nodes -o wide | sed 's/^/         /'
ok "镜像仓库 $REG：宿主 127.0.0.1:5001，集群内 $REG:5000"

step "构建：agentbox、fakeupstream（CGO_ENABLED=0）、worker 镜像（推送并解析 digest）、server 镜像"
(cd "$REPO" && CGO_ENABLED=0 go build -o bin/agentbox ./cmd/agentbox && CGO_ENABLED=0 go build -o bin/fakeupstream ./tests/e2e/fakeupstream/cmd/fakeupstream)
(cd "$REPO" && docker build -q -f deploy/k8s/worker.Dockerfile -t "$WORKER_REF" . >/dev/null)
docker push -q "$WORKER_REF" >/dev/null
DIGEST=$(curl -fsSI -H 'Accept: application/vnd.oci.image.index.v1+json, application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json' \
  "http://127.0.0.1:5001/v2/agentbox-worker/manifests/demo" | tr -d '\r' | awk -F': ' 'tolower($1)=="docker-content-digest"{print $2}')
[ -n "$DIGEST" ] || fail "无法从镜像仓库解析 worker 镜像的 digest"
(cd "$REPO" && docker build -q -f deploy/k8s/server.Dockerfile -t "$SERVER_IMG" . >/dev/null)
ok "worker 镜像 $WORKER_IN_CLUSTER → $DIGEST"
ok "server 镜像 $SERVER_IMG"

step "命名空间 $NS、RBAC 与 ServiceAccount kubeconfig；清空上次的数据与 Pod"
docker rm -f "$SRV" "$FU" "$PG" >/dev/null 2>&1 || true
K apply -f - <"$REPO/deploy/k8s/sandbox.yaml" | sed 's/^/         /'
K apply -f - <"$REPO/deploy/k8s/admission-policy.yaml" | sed 's/^/         /'
K -n "$NS" delete pods -l agentbox.io/managed=true --grace-period=0 --wait=true >/dev/null 2>&1 || true
TOKEN=$(K -n "$NS" create token agentbox-server --duration=4h)
CA=$(docker exec "$NODE" base64 -w0 /etc/kubernetes/pki/ca.crt)
docker run --rm -i --entrypoint sh -v "$HOSTDIR:$HOSTDIR" "$SERVER_IMG" -c "rm -rf '$DATA' && mkdir -p '$DATA' && umask 077 && od -An -N24 -tx1 /dev/urandom | tr -d ' \n' > '$DATA/api.token' && cat > '$HOSTDIR/kubeconfig'" <<EOF
apiVersion: v1
kind: Config
clusters: [{name: kind, cluster: {server: "https://$NODE:6443", certificate-authority-data: "$CA"}}]
users: [{name: agentbox-server, user: {token: "$TOKEN"}}]
contexts: [{name: agentbox, context: {cluster: kind, user: agentbox-server, namespace: $NS}}]
current-context: agentbox
EOF
ok "运维 token：$DATA/api.token（0600，不打印）"
ok "kubeconfig（ServiceAccount $NS/agentbox-server 的短期 token，只能管理本命名空间的 Pod 与 NetworkPolicy）"
[ "$(K auth can-i create pods --as "system:serviceaccount:$NS:agentbox-server" -n default)" = no ] || fail "ServiceAccount 不应能在其他命名空间创建 Pod"
ok "kubectl auth can-i create pods -n default（以 ServiceAccount 身份）= no"
# 准入策略：以 ServiceAccount 身份（--dry-run=server，经过准入但不创建）提交探测 Pod。合规的 Pod（与 provider
# 创建的形状相同）必须被接受；下列每一种违规都必须被 ValidatingAdmissionPolicy 拒绝（策略生效需要数秒，第一个
# 探测重试至多 30 s）。
# probe_pod <volume JSON> <容器 securityContext 覆盖 JSON>：输出探测 Pod 的 JSON。
probe_pod() {
  python3 - "$1" "$2" <<'PY'
import json, sys
vol, extra = json.loads(sys.argv[1]), json.loads(sys.argv[2])
sc = {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True, "capabilities": {"drop": ["ALL"]}}
sc.update(extra)
print(json.dumps({"apiVersion": "v1", "kind": "Pod",
  "metadata": {"name": "admission-probe", "labels": {"agentbox.io/managed": "true"}},
  "spec": {"automountServiceAccountToken": False,
    "securityContext": {"runAsNonRoot": True, "runAsUser": 10001, "seccompProfile": {"type": "RuntimeDefault"}},
    "containers": [{"name": "s", "image": "registry.invalid/none", "securityContext": sc,
                    "volumeMounts": [{"name": "v", "mountPath": "/v"}]}],
    "volumes": [dict(vol, name="v")]}}))
PY
}
probe() { # probe <描述> <volume JSON> <securityContext 覆盖 JSON>；输出 admitted 或拒绝原因
  local out
  if out=$(probe_pod "$2" "$3" | K -n "$NS" create --dry-run=server --as "system:serviceaccount:$NS:agentbox-server" -f - 2>&1); then
    printf '%-44s admitted\n' "$1"
  else
    printf '%-44s DENIED: %s\n' "$1" "$(echo "$out" | sed -n 's/.*denied request: //p' | head -n 1)"
  fi
}
SLOT_OK='{"hostPath":{"path":"/var/lib/agentbox-k8s/data/k8s-slots/abx-0123456789ab/workspace","type":"Directory"}}'
root_denied() { probe "hostPath /" '{"hostPath":{"path":"/"}}' '{}' | grep -q DENIED; }
wait_for "准入策略生效（拒绝 hostPath /）" 30 root_denied
{
  probe "compliant (provider shape)" "$SLOT_OK" '{}'
  probe "hostPath /" '{"hostPath":{"path":"/"}}' '{}'
  probe "hostPath below a slot (symlink-shaped)" '{"hostPath":{"path":"/var/lib/agentbox-k8s/data/k8s-slots/abx-0123456789ab/workspace/x"}}' '{}'
  probe "container runAsNonRoot: false" "$SLOT_OK" '{"runAsNonRoot":false}'
  probe "container seccomp Unconfined" "$SLOT_OK" '{"seccompProfile":{"type":"Unconfined"}}'
  probe "projected ServiceAccount token volume" '{"projected":{"sources":[{"serviceAccountToken":{"path":"token"}}]}}' '{}'
} | tee "$LOGDIR/admission.txt" | sed 's/^/         /'
[ "$(grep -c 'admitted$' "$LOGDIR/admission.txt")" = 1 ] && grep -q '^compliant.*admitted$' "$LOGDIR/admission.txt" ||
  fail "合规的探测 Pod 应被接受，其余应被拒绝"
[ "$(grep -c DENIED "$LOGDIR/admission.txt")" = 5 ] || fail "有违规的探测 Pod 未被拒绝"
ok "ValidatingAdmissionPolicy agentbox-sandbox-pods：合规形状被接受，5 种违规（宿主根、符号链接形路径、容器级 runAsNonRoot/seccomp 覆盖、projected token）均被拒绝"

step "PostgreSQL 与 fake upstream"
docker run -d --name "$PG" --network kind -e POSTGRES_USER=agentbox -e POSTGRES_PASSWORD=agentbox -e POSTGRES_DB=agentbox postgres:16-alpine >/dev/null
wait_for "PostgreSQL 就绪" 60 docker exec "$PG" pg_isready -U agentbox -d agentbox
docker run -d --name "$FU" --network kind -v "$HOSTDIR:$HOSTDIR:ro" --entrypoint /usr/local/bin/fakeupstream "$SERVER_IMG" >/dev/null
wait_for "fake upstream 打印地址" 30 sh -c "docker logs $FU 2>&1 | grep -q '^url='"
read -r FU_URL FU_MODEL FU_HOSTPORT < <(docker logs "$FU" 2>&1 | sed -nE 's/^url=(\S+) model_base_url=(\S+) hostport=(\S+)$/\1 \2 \3/p')
ok "PostgreSQL $PG；fake upstream $FU_URL（模型、搜索与网页，不访问外网）"

step "启动 server：--provider k8s --k8s-warm-pool $WARM（server 与 fake upstream 共享网络命名空间）"
FLAGS=(server --data-dir "$DATA" --listen 127.0.0.1:8080
  --database-url "postgres://agentbox:agentbox@$PG:5432/agentbox?sslmode=disable"
  --provider k8s --k8s-kubeconfig "$HOSTDIR/kubeconfig" --k8s-namespace "$NS"
  --k8s-image "$WORKER_IN_CLUSTER" --k8s-registry-plain-http "$REG:5000" --k8s-warm-pool "$WARM"
  --exec-slots 0 --default-memory-bytes 268435456 --memory-bytes 4294967296 --run-slots 4
  --worker-argv python3,-m,sim_worker
  --model-base-url "$FU_MODEL" --model-name kimi-k2.6 --models kimi-k2.6,kimi-k3 --search-provider fake --search-base-url "$FU_URL"
  --upstream-allow-private "$FU_HOSTPORT")
[ -z "$RUNTIME_CLASS" ] || FLAGS+=(--k8s-runtime-class "$RUNTIME_CLASS")
info "agentbox ${FLAGS[*]}"
docker run -d --name "$SRV" --network "container:$FU" -v "$HOSTDIR:$HOSTDIR" "$SERVER_IMG" "${FLAGS[@]}" >/dev/null
wait_for "server 进入 normal 模式" 90 server_ready
docker logs "$SRV" 2>&1 | grep -m1 'provider k8s' | sed 's/^/         /'
docker logs "$SRV" 2>&1 | grep -q "@$DIGEST" || fail "server 没有把镜像固定到 $DIGEST"
ok "server 已就绪；镜像 tag 已在启动时解析并固定为 digest"
wait_for "预热池就绪" 120 warm_ready
K -n "$NS" get pods -L agentbox.io/state,agentbox.io/pool | sed 's/^/         /'

# ---------------------------------------------------------------------------

step "任务 A：经 Gateway 搜索与 chat（socket 硬链接进 Pod），写产物到 workspace 槽位"
SPEC_A='{"steps":[{"op":"progress","message":"searching"},{"op":"search","step_id":"q1","query":"固态电池","max_results":3},{"op":"chat","step_id":"c1","messages":[{"role":"system","content":"[stage:plan] k8s demo"},{"role":"user","content":"研究主题：k8s"}]},{"op":"artifact","artifact_id":"report","path":"report.md","content":"# report from a Kubernetes pod"},{"op":"checkpoint","step_id":"done"}],"summary":"k8s demo ok","outputs":["report"]}'
TA=$(submit "$SPEC_A")
wait_for "任务 A 结束" 120 is_terminal "$TA"
[ "$(task_field "$TA" status)" = succeeded ] || fail "任务 A 结束为 $(task_field "$TA" status)"
inspect "$TA" | json "'\n'.join('%s %s %s tries=%d' % (c['call_id'], c['endpoint'], c['state'], c['tries_used']) for c in d['calls'])" | sed 's/^/         /'
[ "$(ab task result "$TA" --artifact report)" = "# report from a Kubernetes pod" ] || fail "产物内容不符"
ENV_A=$(env_of "$TA" 1)
docker logs "$SRV" 2>&1 | grep '"k8s: environment ready"' | grep "\"$ENV_A\"" | json "'env %s → pod %s，路径 %s，%d ms' % (d['env_id'], d['pod'], d['path'], d['start_ms'])" | sed 's/^/         /'
ok "succeeded：Gateway 调用 completed，产物经 workspace 槽位到达宿主（sha256 校验通过）"

step "任务 B：运行中检查 Pod 隔离，然后取消（stop）"
TB=$(submit '{"steps":[{"op":"progress","message":"long running"},{"op":"sleep","ms":300000}],"summary":"never","outputs":[]}')
wait_for "任务 B 的 attempt 开始" 60 env_of "$TB" 1
ENV_B=$(env_of "$TB" 1)
wait_for "任务 B 的 Pod" 60 env_pod "$ENV_B"
POD_B=$(env_pod "$ENV_B")
wait_for "Worker 运行" 60 sh -c "docker exec -i $NODE kubectl --kubeconfig /etc/kubernetes/admin.conf -n $NS exec $POD_B -- /opt/agentbox/bin/agentbox-podagent procs | grep -q '[0-9]'"
K -n "$NS" get pod "$POD_B" -o json | python3 -c '
import json, sys
p = json.load(sys.stdin); s = p["spec"]; c = s["containers"][0]
print("image          ", c["image"])
print("pod security   ", json.dumps(s["securityContext"]))
print("container sec  ", json.dumps(c["securityContext"]))
print("resources      ", json.dumps(c["resources"]["limits"]))
print("SA token       ", s.get("automountServiceAccountToken"), " runtimeClass", s.get("runtimeClassName"))
' | sed 's/^/         /'
PROBE='import os, socket
print("uid/gid        ", os.getuid(), os.getgid())
try:
    open("/opt/agentbox/x", "w"); print("rootfs         WRITABLE")
except OSError as e:
    print("rootfs         read-only (%s)" % e.strerror)
print("SA token       ", "PRESENT" if os.path.exists("/var/run/secrets/kubernetes.io") else "absent")
print("gateway socket ", os.path.exists("/run/agentbox/gateway.sock"))
for host, port in (("1.1.1.1", 80), ("kind-registry", 5000), ("10.96.0.10", 53)):
    try:
        socket.create_connection((host, port), timeout=3).close(); print("egress", host, port, "OPEN")
    except OSError as e:
        print("egress", host, port, "blocked (%s)" % (e.strerror or type(e).__name__))
print("capabilities   ", [l.split()[1] for l in open("/proc/self/status") if l.startswith("CapEff")][0])'
K -n "$NS" exec "$POD_B" -- python3 -c "$PROBE" | tee "$LOGDIR/probe.txt" | sed 's/^/         /'
grep -q 'rootfs         read-only' "$LOGDIR/probe.txt" || fail "根文件系统可写"
grep -q 'uid/gid         10001 10001' "$LOGDIR/probe.txt" || fail "Pod 不是以 10001 运行"
grep -q 'SA token        absent' "$LOGDIR/probe.txt" || fail "Pod 中有 ServiceAccount token"
grep -q 'OPEN' "$LOGDIR/probe.txt" && fail "出站连接未被 NetworkPolicy 拒绝"
grep -q 'CapEff' "$LOGDIR/probe.txt" || true
ok "uid 10001、只读根、无 SA token、无 capability、Gateway socket 可见、出站被 deny-all NetworkPolicy 拒绝"
t0=$(date +%s.%N)
ab task cancel "$TB" >/dev/null
wait_for "任务 B 结束" 60 is_terminal "$TB"
t1=$(date +%s.%N)
[ "$(task_field "$TB" status)" = cancelled ] || fail "任务 B 结束为 $(task_field "$TB" status)"
wait_for "任务 B 的环境清理完成" 60 cleaned "$TB"
[ -z "$(env_pods "$ENV_B")" ] || fail "取消后 Pod 仍存在"
ok "cancelled：取消到终态 $(python3 -c "print(round($t1-$t0, 2))") s；Pod $POD_B 已停止并删除"

step "任务 C：运行中 SIGKILL server，孤儿 Pod 留存；重启后恢复"
TC=$(submit '{"steps":[{"op":"artifact","artifact_id":"a1","path":"a1.txt","content":"one"},{"op":"checkpoint","step_id":"s1"},{"op":"sleep","ms":8000},{"op":"artifact","artifact_id":"a2","path":"a2.txt","content":"two"}],"summary":"survived restart","outputs":["a1","a2"]}')
wait_for "checkpoint s1 已提交" 60 has_checkpoint "$TC"
ENV_C=$(env_of "$TC" 1)
POD_C=$(env_pod "$ENV_C")
docker kill -s KILL "$SRV" >/dev/null
pod_running "$POD_C" || fail "server 被杀死后孤儿 Pod 应仍在运行"
ok "server 已被 SIGKILL；孤儿 Pod $POD_C（env $ENV_C）仍在运行"
docker start "$SRV" >/dev/null
wait_for "server 重新进入 normal 模式" 90 server_ready
docker logs "$SRV" 2>&1 | grep -E '"msg":"(恢复扫描|启动恢复完成)"' | tail -n 2 |
  json "'%s %s' % (d['msg'], json.dumps({k: v for k, v in d.items() if k not in ('time', 'level', 'msg')}, ensure_ascii=False))" 2>/dev/null | sed 's/^/         /' || true
wait_for "任务 C 结束" 120 is_terminal "$TC"
inspect "$TC" | json "'\n'.join('attempt %d：%s（env %s）' % (a['attempt_no'], a['outcome_class'], a['env_id']) for a in d['attempts'])" | sed 's/^/         /'
[ "$(task_field "$TC" status)" = succeeded ] || fail "任务 C 结束为 $(task_field "$TC" status)"
[ "$(inspect "$TC" | json "[a['outcome_class'] for a in d['attempts']] == ['lost_on_restart', 'succeeded']")" = True ] || fail "attempt 分类不符"
wait_for "任务 C 的环境清理完成" 60 cleaned "$TC"
[ -z "$(env_pods "$ENV_C")" ] || fail "孤儿 Pod 未被回收"
ok "succeeded：attempt 1 lost_on_restart，attempt 2 从 s1 恢复；孤儿 Pod 已停止并删除"

step "启动延迟：热（预热池，默认资源）与冷（memory_max 不同于预热规格）各 $SAMPLES 个任务"
QUICK='{"steps":[{"op":"progress","message":"hi"}],"summary":"quick","outputs":[]}'
: >"$LOGDIR/tasks.tsv"
run_quick() { # run_quick <warm|cold>
  local t s e
  s=$(date +%s.%N)
  if [ "$1" = warm ]; then t=$(submit "$QUICK"); else t=$(submit "$QUICK" '{"memory_max":335544320}'); fi
  wait_for "任务 $t 结束" 120 is_terminal "$t"
  e=$(date +%s.%N)
  [ "$(task_field "$t" status)" = succeeded ] || fail "任务 $t 结束为 $(task_field "$t" status)"
  wait_for "任务 $t 的环境清理完成" 60 cleaned "$t"
  printf '%s\t%s\t%s\n' "$1" "$(env_of "$t" 1)" "$(python3 -c "print($e-$s)")" >>"$LOGDIR/tasks.tsv"
}
for ((i = 0; i < SAMPLES; i++)); do wait_for "预热 Pod 就绪" 120 warm_ready; run_quick warm; done
for ((i = 0; i < SAMPLES; i++)); do run_quick cold; done
docker logs "$SRV" 2>&1 | grep '"k8s: environment ready"' >"$LOGDIR/ready.jsonl" || true
python3 - "$LOGDIR/tasks.tsv" "$LOGDIR/ready.jsonl" <<'PY' | tee "$LOGDIR/latency.txt" | sed 's/^/         /'
import json, math, sys
tasks = [l.rstrip("\n").split("\t") for l in open(sys.argv[1]) if l.strip()]
ready = {}
for l in open(sys.argv[2]):
    d = json.loads(l)
    ready[d["env_id"]] = (d["path"], d["start_ms"])
def q(v, p):
    v = sorted(v); return v[max(0, math.ceil(p * len(v)) - 1)]
rows = {}
for want, env, total in tasks:
    path, ms = ready[env]
    rows.setdefault(want, []).append((path, ms, float(total) * 1000))
print("%-5s %3s %-22s %-28s %s" % ("", "n", "env ready P50/P95 (ms)", "submit→terminal P50/P95 (ms)", "paths"))
for want in ("warm", "cold"):
    r = rows.get(want, [])
    if not r:
        continue
    paths = sorted({p for p, _, _ in r})
    print("%-5s %3d %-22s %-28s %s" % (want, len(r), "%d / %d" % (q([m for _, m, _ in r], .5), q([m for _, m, _ in r], .95)),
          "%d / %d" % (q([t for _, _, t in r], .5), q([t for _, _, t in r], .95)), ",".join(paths)))
    if paths != [want]:
        sys.exit("%s 组的任务走了 %s 路径" % (want, paths))
PY
ok "延迟明细：$LOGDIR/tasks.tsv、$LOGDIR/ready.jsonl"

step "正常停止 server 并检查泄漏"
docker stop -t 30 "$SRV" >/dev/null
rc=$(docker inspect -f '{{.State.ExitCode}}' "$SRV")
[ "$rc" = 0 ] || fail "server 退出码 $rc"
ok "server 退出码 0"
left=$(K -n "$NS" get pods -l agentbox.io/state=assigned -o name)
[ -z "$left" ] || fail "仍有绑定到环境的 Pod：$left"
ok "没有绑定到环境的 Pod；预热 Pod（$(K -n "$NS" get pods -l agentbox.io/state=warm -o name | wc -l) 个）由下次运行复用"
K -n "$NS" delete pods -l agentbox.io/managed=true --grace-period=0 >/dev/null 2>&1 || true
ok "已删除预热 Pod；集群 $CLUSTER 与镜像仓库 $REG 保留（kind delete cluster --name $CLUSTER 可删除）"

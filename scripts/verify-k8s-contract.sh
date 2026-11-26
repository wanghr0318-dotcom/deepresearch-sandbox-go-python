#!/usr/bin/env bash
# Run the real K8s Provider contract and require an unskipped verdict.
set -euo pipefail

: "${AGENTBOX_K8S_TEST_KUBECONFIG:?set a kubeconfig for the dedicated acceptance cluster}"
: "${AGENTBOX_K8S_TEST_IMAGE:?set the pinned worker image}"
if [[ $# -ne 1 ]]; then
  echo "usage: bash scripts/verify-k8s-contract.sh <report.jsonl>" >&2
  exit 2
fi
report_dir=$(cd "$(dirname -- "$1")" && pwd)
report="$report_dir/$(basename -- "$1")"
cd "$(dirname -- "${BASH_SOURCE[0]}")/.."

go test -json ./internal/provider/k8s -run '^TestConformanceOnRealCluster$' -count=1 | tee "$report"
go run ./tools/citestjson "$report" /dev/null

# Worker image for the Kubernetes provider (docs/design/2026-10-10-k8s-provider-design.md §2.1, §2.5).
# Build from the repository root:
#   docker build -f deploy/k8s/worker.Dockerfile -t <registry>/agentbox-worker:<tag> .
# The provider pins the pushed image by digest; base images are pinned by digest as well.

FROM docker.io/library/golang:1.27.2-alpine@sha256:f92b6ef800e499660581efdabdf25d9d817a9d124eaf900924f0504e7e27e12d AS podagent
WORKDIR /src
COPY go.mod go.sum ./
COPY cmd/agentbox-podagent ./cmd/agentbox-podagent
COPY internal/provider/k8s/podapi ./internal/provider/k8s/podapi
# The helper imports only the standard library and podapi, so no module download is needed.
RUN CGO_ENABLED=0 GOFLAGS=-mod=mod GOPROXY=off go build -trimpath -ldflags=-s -o /out/agentbox-podagent ./cmd/agentbox-podagent

FROM docker.io/library/python:3.12-slim@sha256:a6e34c598f2467ed0e9a8d349809fcd8b5c603269512df273a0bb1784edc11b1
# Same layout as the local provider's rootfs template: the worker packages live in /opt/agentbox
# (rootfs.WorkerDir) and are imported through PYTHONPATH set by the server (--worker-env).
COPY worker/agentbox_worker /opt/agentbox/agentbox_worker
COPY worker/sim_worker /opt/agentbox/sim_worker
COPY worker/deepresearch /opt/agentbox/deepresearch
COPY worker/chatagent /opt/agentbox/chatagent
COPY worker/skills /opt/agentbox/skills
COPY --from=podagent /out/agentbox-podagent /opt/agentbox/bin/agentbox-podagent
# Byte-compile at build time: the root filesystem is read-only at run time.
RUN python3 -m compileall -q /opt/agentbox && mkdir -p /workspace /run/agentbox /run/agentbox-exec
ENV PYTHONDONTWRITEBYTECODE=1
USER 10001:10001
ENTRYPOINT ["/opt/agentbox/bin/agentbox-podagent", "init"]

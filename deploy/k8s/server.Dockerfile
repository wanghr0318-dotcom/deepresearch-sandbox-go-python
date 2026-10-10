# Demo-only image running the agentbox server and the fake upstream next to a kind cluster
# (scripts/demo-k8s.sh). The binaries are built beforehand with CGO_ENABLED=0 into bin/.
FROM python:3.12-slim@sha256:a6e34c598f2467ed0e9a8d349809fcd8b5c603269512df273a0bb1784edc11b1
COPY bin/agentbox bin/fakeupstream /usr/local/bin/
ENTRYPOINT ["/usr/local/bin/agentbox"]

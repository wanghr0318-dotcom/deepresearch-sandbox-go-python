# Phase-four recovery dashboard

Grafana filters the task tables by round and task. The collector reads task events, PostgreSQL observations, slot admission timestamps and grading records. A task spanning two recovery hops contributes one task identity and one grade. Each hop has its own time origin and event rows.

Install the three Python modules matching `eval/phase4/dashboard_*.py` on the experiment controller. The collector scans `PHASE4_RUN_ROOT` (default `/var/tmp`) and publishes `public/metrics` under `PHASE4_STATE`. Live PostgreSQL observations use the experiment database and namespace naming conventions in the collector. The example exporter service binds to loopback. For cross-node scraping, serve the metrics file on a private interface with access limited to the monitoring host.

On the monitoring host, copy `prom-c.example.yml` to `/opt/agentbox-phase4-monitor/prom.yml` and replace the example hostnames with the private addresses of the two experiment nodes. Run `promtool check config` before loading it. The five targets are the task collector, two node_exporters, Grafana and Prometheus.

Install `grafana.ini` and the provisioning directory under `/opt/agentbox-phase4-monitor/`. Copy `../grafana/dashboards/agentbox-phase4.json` into the provisioned boards directory and `../phase4-live.html` into the Grafana public directory. Supply administrator settings through the private file referenced in the Grafana service. Grafana listens on loopback and permits anonymous Viewer access there. The Prometheus unit retains three days of samples.

Forward local port 13006 over SSH to the monitoring host's loopback port 3000. The lightweight dashboard is at `/public/phase4-live.html` and the Grafana dashboard has UID `agentbox-phase4`.

~~~sh
python3 eval/phase4/dashboard_archive.py archive /var/tmp /var/lib/agentbox-phase4-monitor/pg inputs.jsonl.gz
python3 eval/phase4/dashboard_archive.py replay inputs.jsonl.gz replay --html-template deploy/observability/phase4-live.html
~~~

Open `replay/task-dashboard.html` locally. Offline replay reconstructs task evidence without contacting the cluster. Host resource samples remain in Prometheus. Archives may contain hostnames and task identifiers; keep them with the original experiment evidence.

## Rebuild from packages and a task archive

On Ubuntu amd64 with Python 3.11+ and dpkg-deb, run the following from the repository root. Supply the matching Grafana and Prometheus Debian packages, their independently recorded SHA-256 values, the archive, and the two private node_exporter endpoints. The destination must be new. The experiment hostnames in this example are placeholders.

~~~sh
python3 deploy/observability/phase4/rebuild.py \
  --output "$HOME/monitor-rebuild" --archive inputs.jsonl.gz \
  --grafana-deb "$GRAFANA_DEB" --grafana-sha256 "$GRAFANA_SHA256" \
  --prometheus-deb "$PROMETHEUS_DEB" --prometheus-sha256 "$PROMETHEUS_SHA256" \
  --node-a node-a.internal.example:9100 --node-b node-b.internal.example:9100
~~~

The command extracts both packages, generates a private administrator password, provisions the data source and dashboard, and starts new databases on dynamically selected loopback ports. It checks all five targets, the archived task identities and administrator authentication before shutting down the test processes. Read `monitor-rebuild/proof.json` for the outcome; the administrator credential stays in a separate private file.

For a continuously running stack, append `--serve` and select stable, free ports with `--grafana-port` and `--prometheus-port`. Supply `--task-target controller.internal.example:19467` to read live task records. With the default task source, the installed dashboard serves the replayed archive. Configure the included systemd units separately for machine boot, using the actual installation paths. Historical host-resource samples require a TSDB backup; the task archive restores task events and scores.

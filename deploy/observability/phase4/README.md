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

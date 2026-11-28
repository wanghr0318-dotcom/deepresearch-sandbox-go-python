# Independent writer and single-attempt test adapter

This experiment uses a PostgreSQL test database, a loopback test adapter on A, and a separate Linux writer process on B. The adapter creates a fixed synthetic task with a 120-second lease. The token only authorizes fixture operations on that synthetic old attempt. Keep the management and transport evidence with the experiment log.

## Start the adapter

Set `AGENTBOX_TEST_DATABASE_URL` in A's private test environment. The test database role needs permission to create and drop the disposable database created by the Go fixture. Prepare a fresh private directory on A, generate a one-round random token into a private file, and transfer that token through the existing private management channel to B's private writer directory as `token`. The adapter requires at least 32 characters after trimming whitespace.

~~~sh
umask 077
export AGENTBOX_SINGLE_ATTEMPT_DIR="$(mktemp -d)"
export AGENTBOX_SINGLE_ATTEMPT_TOKEN_FILE="$AGENTBOX_SINGLE_ATTEMPT_DIR/token"
python3 -c 'import os,secrets,pathlib; pathlib.Path(os.environ["AGENTBOX_SINGLE_ATTEMPT_TOKEN_FILE"]).write_text(secrets.token_hex(32))'
export AGENTBOX_SINGLE_ATTEMPT_PORT=19489
go test ./internal/persistence/postgres -run '^TestSingleAttemptCloudAdapter$' -count=1 -timeout=16m -v
~~~

The adapter binds only to A's loopback interface and writes `ledger.jsonl` in the fresh directory. The management connection must remain available while the adapter's dedicated data forward can be stopped and restored. If the optional port setting is omitted, A listens on 19489.

## Start the writer and collect evidence

Choose a B-local loopback port for a dedicated forward to the adapter and set `WRITER_DIR` to B's private directory. With that forward active, run the writer as a dedicated independent service. For example, from a user account with a running systemd user manager:

~~~sh
systemd-run --user --unit="$WRITER_UNIT" --property=Type=exec \
  /usr/bin/python3 -u "$REPO/eval/phase4/independent_writer.py" \
  --state-dir "$WRITER_DIR" --port "$B_LOCAL_PORT"
~~~

Use a fresh unit name for each round. The writer logs its host, boot ID, PID and start ticks, followed by periodic heartbeats and operation IDs. Waiting for `bound` proves it acquired the fixture identity before any interruption. Existing installations may continue to use the original defaults.

1. Record the initial two-node Ready map and old PG owner, along with B's process witness. Stop only the dedicated data forward. Mark the cut with `cut.flag` under B's writer directory and confirm that B remains reachable over the separate management connection.
2. Once the data path is down, create an empty `start` file in A's adapter directory. The adapter records its last renewal, waits for PG expiry, creates a successor for this fixture and snapshots its state. Confirm the successor in PG and the same live B process through management before restoring the test forward.
3. The same writer records responses for renewal, checkpoint, file head and final result. Once the writer logs `complete`, signal `finish` in A's adapter directory. Preserve its ledger and copy B's journal with `journalctl --user -u "$WRITER_UNIT" -o cat`. Independently record final node readiness.

The operator's transport JSONL supplies the ordered setup/cut/confirmation/restoration witnesses specified in `verify_transport`. The checker requires B's identity to agree across the transport witnesses and writer journal, plus both setup nodes to be Ready in the final node map.

~~~sh
python3 eval/phase4/verify_independent_writer.py writer.jsonl ledger.jsonl transport.jsonl final-nodes.json
~~~

Report attempts, connection failures, entry rejections, PostgreSQL rejections and accepted operations per write type. A missing operation supplies no rejection evidence. This opt-in test covers the independent fixture writer and the successor's persisted state. The cloud node lifecycle and the real worker process are measured in separate experiments.

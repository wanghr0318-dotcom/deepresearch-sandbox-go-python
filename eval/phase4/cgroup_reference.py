"""Run the five-submission CPU reference with the same active lease as node loss."""
import sys
from types import SimpleNamespace

from batch import BASE, FIXTURES, accept_batch, steps_for, verify, worker_command

if __name__ == "__main__":
    import argparse
    cli = argparse.ArgumentParser(add_help=False)
    cli.add_argument("--source-node",required=True)
    cli.add_argument("--target-node",required=True)
    p,remaining = cli.parse_known_args()
    opts = SimpleNamespace(mode="reference",tasks=5,session_recovery_slots=2,lease_ttl="120s",
                           allow_spillover=True,check_egress=False,real_model=False,**vars(p))
    sys.argv=[sys.argv[0],*remaining,"--scripted","--fixture",str(FIXTURES["csv-first-row"])]
    flags=["--k8s-preferred-node",p.source_node,"--session-recovery-slots","2",
           "--allow-session-lease-fence","--attempt-lease-ttl","120s"]
    path=BASE["main"](worker_command=lambda _:worker_command(False),run_slots=5,gateway_flags=flags,
                      upstream=lambda:BASE["tool_upstream"](steps_for),
                      acceptance=lambda *a:accept_batch(opts,*a))
    verify(path)

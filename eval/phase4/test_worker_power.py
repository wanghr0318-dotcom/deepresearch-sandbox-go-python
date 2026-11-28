"""Exercise power-safety and idempotence without changing a live node."""
import json
import unittest
from unittest.mock import patch

from stop_worker import restore_worker


class WorkerRecoveryTest(unittest.TestCase):
    def run_case(self, states, *, control=False, mismatch=False):
        actions=[]
        states=iter(states)
        def output(args, **_):
            if "tccli" == args[0]:
                action=args[2]
                actions.append(action)
                if action=="DescribeInstances":
                    return json.dumps({"InstanceSet":[{"InstanceId":"ins-sample","InstanceState":next(states),
                                       "PrivateIpAddresses":["192.0.2.99" if mismatch else "192.0.2.2"]}]}).encode()
                self.assertEqual(action,"StartInstances")
                return b'{"RequestId":"start-request"}'
            if "nodes" in args:
                return json.dumps({"items":[{"metadata":{"name":s},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
                                             for s in ["worker","peer"]]}).encode()
            if "--raw" in args:
                return b"ok"
            return json.dumps({"metadata":{"labels":{"node-role.kubernetes.io/control-plane":""} if control else {}},
                               "status":{"addresses":[{"type":"InternalIP","address":"192.0.2.2"}]}}).encode()
        self.actions=actions
        with patch("stop_worker.subprocess.check_output",side_effect=output), patch("stop_worker.time.sleep"):
            return restore_worker("ins-sample","worker","peer","sample-region")

    def test_stopped_then_started(self):
        result=self.run_case(["STOPPING","STOPPED","STARTING","RUNNING","RUNNING"])
        self.assertEqual(self.actions.count("StartInstances"),1)
        self.assertEqual(result["start_request"],"start-request")
        self.assertEqual(result["kubelet"],"ok")

    def test_running_needs_no_start(self):
        self.assertIsNone(self.run_case(["RUNNING","RUNNING"])["start_request"])
        self.assertNotIn("StartInstances",self.actions)

    def test_protect_control_plane(self):
        with self.assertRaisesRegex(ValueError,"control-plane"):
            self.run_case([],control=True)
        self.assertFalse(self.actions)

    def test_reject_wrong_identity(self):
        with self.assertRaisesRegex(ValueError,"internal IP"):
            self.run_case(["STOPPED"],mismatch=True)
        self.assertNotIn("StartInstances",self.actions)


if __name__=="__main__":
    unittest.main()

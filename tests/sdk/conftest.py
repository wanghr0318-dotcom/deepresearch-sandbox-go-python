"""Starts the model API harness (tests/sdk/harness) once per session.

AGENTBOX_SDK_HARNESS is the path of the built harness binary (scripts/sdk-compat.sh builds it); the harness reads
AGENTBOX_TEST_DATABASE_URL itself. Without AGENTBOX_SDK_HARNESS the tests fail rather than skip.
"""

import json
import os
import subprocess

import pytest


@pytest.fixture(scope="session")
def harness():
    path = os.environ.get("AGENTBOX_SDK_HARNESS")
    if not path:
        pytest.fail("AGENTBOX_SDK_HARNESS is not set (build tests/sdk/harness, see scripts/sdk-compat.sh)")
    proc = subprocess.Popen([path], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    try:
        line = proc.stdout.readline()
        if not line:
            pytest.fail(f"harness exited with {proc.wait()}")
        yield json.loads(line)
    finally:
        proc.stdin.close()  # the harness stops at EOF and drops its database
        try:
            proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            proc.kill()

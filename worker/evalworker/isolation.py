"""Process separation between the checker and the solution under test (runs in the exec sandbox).

The checker never imports the solution's code in its own interpreter. `solution.py` next to
check.py is a proxy: on import it starts a child process (`_eval_server.py`) that imports the real
solution from `_eval_impl/`, and every attribute access or call is forwarded over a pair of pipes
as JSON (no pickle; only None, bool, int, float, str, list, tuple, dict, set, frozenset and bytes
cross the boundary). Exceptions come back as the same builtin exception type when it is an
`Exception` subclass, otherwise as `RemoteError`; a `SystemExit` raised by the solution never
becomes a `SystemExit` in the checker. Run as a script (`python solution.py`), the proxy execs the
real solution in its place.

The child never sees the completion token: the checker's prelude reads and closes the token pipe
before the proxy is imported, the token is not in argv or the environment, and the checker and the
harness mark themselves non-dumpable (prctl PR_SET_DUMPABLE 0), so a same-uid process cannot open
/proc/<pid>/mem, environ or fd of either (in the exec sandbox seccomp also denies ptrace and the
only capability is KILL).

These are source texts written into the exec scratch directory, never imported by the worker.
"""

from __future__ import annotations

IMPL_DIR = "_eval_impl"

# Shared JSON codec (tagged values for the non-JSON types).
CODEC_SRC = r"""
import base64
import builtins
import json

MAX_DEPTH = 200


def encode(v, depth=0):
    if depth > MAX_DEPTH:
        raise ValueError("value nested too deeply")
    if v is None or isinstance(v, (bool, int, float, str)):
        return v
    if isinstance(v, list):
        return [encode(x, depth + 1) for x in v]
    if isinstance(v, tuple):
        return {"$t": [encode(x, depth + 1) for x in v]}
    if isinstance(v, dict):
        return {"$d": [[encode(k, depth + 1), encode(x, depth + 1)] for k, x in v.items()]}
    if isinstance(v, frozenset):
        return {"$fs": [encode(x, depth + 1) for x in v]}
    if isinstance(v, set):
        return {"$s": [encode(x, depth + 1) for x in v]}
    if isinstance(v, (bytes, bytearray)):
        return {"$b": base64.b64encode(bytes(v)).decode("ascii")}
    raise TypeError("value of type %s cannot cross the process boundary" % type(v).__name__)


def decode(v):
    if isinstance(v, list):
        return [decode(x) for x in v]
    if isinstance(v, dict):
        if "$t" in v:
            return tuple(decode(x) for x in v["$t"])
        if "$d" in v:
            return {_key(decode(k)): decode(x) for k, x in v["$d"]}
        if "$fs" in v:
            return frozenset(decode(x) for x in v["$fs"])
        if "$s" in v:
            return set(decode(x) for x in v["$s"])
        if "$b" in v:
            return base64.b64decode(v["$b"])
        raise ValueError("unknown tagged value")
    return v


def _key(k):
    if isinstance(k, list):  # lists are unhashable: they cannot have been dict keys
        raise ValueError("invalid dict key")
    return k


def send(f, obj):
    data = json.dumps(obj).encode("utf-8")
    f.write(len(data).to_bytes(8, "big") + data)
    f.flush()


def recv(f):
    head = _read(f, 8)
    if head is None:
        return None
    body = _read(f, int.from_bytes(head, "big"))
    if body is None:
        return None
    return json.loads(body.decode("utf-8"))


def _read(f, n):
    buf = b""
    while len(buf) < n:
        chunk = f.read(n - len(buf))
        if not chunk:
            return None
        buf += chunk
    return buf


def describe(exc):
    return {"type": type(exc).__name__, "message": str(exc)[:4000]}
"""

# Child: imports the real solution and serves get/call requests. Started with -u so that
# the solution's prints reach the checker's stdout in order (before the completion token).
SERVER_SRC = r"""
import os
import sys

rfd, wfd, impl = int(sys.argv[1]), int(sys.argv[2]), sys.argv[3]
here = os.path.dirname(os.path.abspath(__file__))
sys.path[:] = [impl] + [p for p in sys.path if os.path.abspath(p or ".") != here]
sys.path.append(here)  # for the codec only; the real solution in impl wins
import _eval_codec as codec  # noqa: E402

rf = os.fdopen(rfd, "rb", buffering=0)
wf = os.fdopen(wfd, "wb", buffering=0)
try:
    import solution as mod
except BaseException as e:  # noqa: B036 - report any import failure, including SystemExit
    codec.send(wf, {"error": codec.describe(e)})
    raise SystemExit(1)
codec.send(wf, {"ok": True})
while True:
    req = codec.recv(rf)
    if req is None:
        break
    try:
        if req["op"] == "get":
            if not hasattr(mod, req["name"]):
                resp = {"missing": True}
            else:
                v = getattr(mod, req["name"])
                resp = {"callable": True} if callable(v) else {"value": codec.encode(v)}
        else:
            fn = getattr(mod, req["name"])
            result = fn(*codec.decode(req["args"]), **codec.decode(req["kwargs"]))
            resp = {"value": codec.encode(result)}
    except BaseException as e:  # noqa: B036 - forwarded to the checker as an exception
        resp = {"error": codec.describe(e)}
    codec.send(wf, resp)
"""

# Proxy imported by check.py as `solution`.
PROXY_SRC = r'''
import atexit
import builtins
import os
import subprocess
import sys

_here = os.path.dirname(os.path.abspath(__file__))
_impl = os.path.join(_here, "_eval_impl")
if __name__ == "__main__":  # run as a script: become the real solution
    os.execv(sys.executable, [sys.executable, "-E", "-s", "-B", os.path.join(_impl, "solution.py")]
             + sys.argv[1:])

import _eval_codec as _codec  # noqa: E402


class RemoteError(Exception):
    """An exception raised by the solution that is not a builtin Exception subclass."""


def _raise(err):
    t = getattr(builtins, err.get("type", ""), None)
    msg = err.get("message", "")
    if isinstance(t, type) and issubclass(t, Exception) and not issubclass(t, RemoteError):
        try:
            exc = t(msg)
        except Exception:
            exc = RemoteError("%s: %s" % (err.get("type"), msg))
    else:
        exc = RemoteError("%s: %s" % (err.get("type"), msg))
    raise exc


_p2c_r, _p2c_w = os.pipe()
_c2p_r, _c2p_w = os.pipe()
_child = subprocess.Popen(
    [sys.executable, "-E", "-s", "-B", "-u", os.path.join(_here, "_eval_server.py"),
     str(_p2c_r), str(_c2p_w), _impl],
    pass_fds=(_p2c_r, _c2p_w), close_fds=True, stdin=subprocess.DEVNULL)
os.close(_p2c_r)
os.close(_c2p_w)
_to = os.fdopen(_p2c_w, "wb", buffering=0)
_from = os.fdopen(_c2p_r, "rb", buffering=0)


def _stop():
    try:
        _to.close()
    except OSError:
        pass
    try:
        _child.wait(timeout=2)  # EOF on its request pipe ends the child's loop
    except subprocess.TimeoutExpired:
        _child.kill()
        _child.wait()


atexit.register(_stop)


def _rpc(req):
    try:
        if req is not None:
            _codec.send(_to, req)
        resp = _codec.recv(_from)
    except OSError:
        resp = None
    if resp is None:
        raise RuntimeError("the solution process exited unexpectedly")
    if "error" in resp:
        _raise(resp["error"])
    return resp


try:
    _rpc(None)  # the child's import result
except RuntimeError:
    raise ImportError("solution: the solution process exited during import") from None
except Exception as e:
    raise ImportError("solution: importing the solution failed: %s" % e) from None


def _stub(name):
    def call(*args, **kwargs):
        resp = _rpc({"op": "call", "name": name, "args": _codec.encode(list(args)),
                     "kwargs": _codec.encode(dict(kwargs))})
        return _codec.decode(resp["value"])
    call.__name__ = name
    return call


def __getattr__(name):
    if name.startswith("__"):
        raise AttributeError(name)
    resp = _rpc({"op": "get", "name": name})
    if resp.get("missing"):
        raise AttributeError(name)
    if resp.get("callable"):
        return _stub(name)
    return _codec.decode(resp["value"])
'''

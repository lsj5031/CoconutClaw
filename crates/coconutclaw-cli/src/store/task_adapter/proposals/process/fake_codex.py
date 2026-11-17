"""Disposable explicit Codex fixture, no network/auth or ambient configuration."""
import json
import os
import signal
import socket
import struct
import subprocess
import sys
import sqlite3
from pathlib import Path

if sys.platform == "linux":
    import ctypes

    libc = ctypes.CDLL(None)
    libc.prctl(1, signal.SIGTERM, 0, 0, 0)
# On every Unix host, runner death closes the control socket (and a terminated
# leader closes its descendant's stdin). The EOF paths below still reap children.

if sys.argv[1] == "--descendant":
    print("ready", flush=True)
    sys.stdin.buffer.read(1)
    sys.exit(0)

job, assignment, runner = sys.argv[1:4]
if os.getppid() != int(runner):
    sys.exit(91)
args = sys.argv[4:]
behavior = args[0] if args[0] in {"fail", "limit", "oversized"} else "normal"
if behavior != "normal":
    args = args[1:]
assert args[0] == "exec" and args[-1] == "-" and "--json" in args
assert "--dangerously-bypass-approvals-and-sandbox" not in args
allowed_environment = {
    "HOME", "CODEX_HOME", "LC_CTYPE", "FACTORY_AIRGAP_ENABLED",
    "COCONUTCLAW_MOCK_PROCESS_SOCKET",
}
assert set(os.environ) <= allowed_environment, (
    "unexpected environment names: " + ", ".join(sorted(set(os.environ) - allowed_environment))
)
assert os.environ["FACTORY_AIRGAP_ENABLED"] == "1"
root = Path(__file__).parent
with sqlite3.connect("file:" + str(root / "work/state.db") + "?mode=ro", uri=True) as store:
    claim = store.execute(
        "SELECT r.id,r.status FROM process_claim c JOIN task_runs r ON r.id=c.run "
        "WHERE c.assignment=? AND c.pid=? AND c.provider=1", (int(assignment), int(runner))
    ).fetchone()
    assert claim and claim[1] == "running"
with sqlite3.connect("file:" + str(root / "runner.sqlite") + "?mode=ro", uri=True) as journal:
    assert journal.execute("SELECT start FROM responses WHERE assignment=?", (int(assignment),)).fetchone()[0]
prompt = sys.stdin.read()
session = "fixture-session-" + assignment
if args[1] == "resume":
    assert args[2] == session
else:
    assert args[1:3] == ["--cd", os.getcwd()]
out_file = args[args.index("--output-last-message") + 1]
descendant = subprocess.Popen(
    [sys.executable, "-I", __file__, "--descendant"],
    stdin=subprocess.PIPE, stdout=subprocess.PIPE,
)
assert descendant.stdout.readline() == b"ready\n"


def terminate(_signum, _frame):
    raise SystemExit(130)


signal.signal(signal.SIGTERM, terminate)
try:
    with socket.socket(socket.AF_UNIX) as control:
        control.connect(job + "/control.sock")
        observed = dict(
            pid=os.getpid(), parent=os.getppid(), group=os.getpgrp(),
            descendant=descendant.pid, prompt=prompt, cwd=os.getcwd(),
            home=os.environ["HOME"], codex_home=os.environ["CODEX_HOME"],
            args=args, environment=sorted(os.environ),
            claim_run=claim[0], durable_start=True,
        )
        body = json.dumps(observed).encode()
        control.sendall(struct.pack(">I", len(body)) + body)
        print(json.dumps(dict(type="thread.started", thread_id=session)), flush=True)
        print(json.dumps(dict(type="turn.started")), flush=True)
        if control.recv(1) != b"\x01":
            sys.exit(92)
        if behavior == "fail":
            print("explicit fake failure", file=sys.stderr, flush=True)
            sys.exit(7)
        with open(out_file, "x") as output:
            output.write("B" * (2048 if behavior == "limit" else 2049)
                         if behavior in {"limit", "oversized"}
                         else "fake assignment " + assignment + ": " + prompt)
        print(json.dumps(dict(type="item.completed", item=dict(type="agent_message", text="done"))), flush=True)
finally:
    descendant.terminate()
    descendant.wait()

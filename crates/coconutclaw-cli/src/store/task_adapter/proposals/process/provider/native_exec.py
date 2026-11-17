"""Owned native wrapper: local auth in memory, isolated native state, parent lease."""
import json
import os
import signal
import socket
import subprocess
import sys
import threading
from pathlib import Path

profile, job = Path(sys.argv[1]), Path(sys.argv[2])
cfg = json.loads(profile.read_text())
key = None
if not cfg.get("auth_command_in_place", False):
    auth = json.loads(Path(cfg["auth_json"]).read_text())
    key = auth.get("OPENAI_API_KEY")
    if not isinstance(key, str) or not key:
        raise SystemExit("Existing local API-key auth unavailable; no login or fallback")
keep = {"PATH", "HOME", "TMPDIR", "LANG", "LC_ALL", "SSL_CERT_FILE",
        "SSL_CERT_DIR", "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY"}
for name in list(os.environ):
    if name not in keep:
        del os.environ[name]
if key is not None:
    os.environ["COCONUTCLAW_ACCEPTANCE_API_KEY"] = key
os.environ["CODEX_HOME"] = str(job / "codex-home")
group = os.getpgrp()
if group != os.getpid():
    raise SystemExit("Native wrapper must own its provider process group")
lease = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
lease.connect(str(job / "guardian.sock"))
record = {
    "version": 1, "pid": os.getpid(), "parent": os.getppid(), "group": group,
    "hostname": socket.gethostname(), "cwd": os.getcwd(),
}
with (job / "launch.json").open("x") as out:
    json.dump(record, out)
    out.flush()
    os.fsync(out.fileno())
child = subprocess.Popen([cfg["executable"], *sys.argv[3:]])
finished = threading.Event()
lost_parent = threading.Event()


def guard():
    try:
        lease.recv(1)
    except OSError:
        pass
    if not finished.is_set():
        # This wrapper is still the live group leader, not a reused journal PID.
        # Allow the native CLI to clean up tools, then kill this owned group.
        lost_parent.set()
        os.killpg(group, signal.SIGTERM)
        try:
            child.wait(timeout=2)
        except subprocess.TimeoutExpired:
            pass
        os.killpg(group, signal.SIGKILL)


# Ignore TERM in the guardian only; the native child inherited normal handling.
signal.signal(signal.SIGTERM, signal.SIG_IGN)
watcher = threading.Thread(target=guard, daemon=True)
watcher.start()
try:
    code = child.wait()
finally:
    if lost_parent.is_set():
        watcher.join()
    finished.set()
    lease.close()
raise SystemExit(code)

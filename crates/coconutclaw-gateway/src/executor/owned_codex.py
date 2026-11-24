import ctypes
import json
import os
import select
import signal
import stat
import subprocess
import sys
import tempfile
import time
from pathlib import Path


def live(deadline):
    return (time.time_ns() // 1_000_000 < deadline["wall"]
            and time.monotonic_ns() // 1_000_000 < deadline["monotonic"])


def require_spawn_authority(deadline, parent):
    if not live(deadline) or parent.poll(0):
        raise TimeoutError("launch authority or runner lease expired")


def emit(event, **fields):
    print(json.dumps({"event": event, **fields}), flush=True)


def signal_owned(pid, deadline=None):
    try:
        descriptor = os.pidfd_open(pid)
    except ProcessLookupError:
        return False
    try:
        if deadline is not None and not live(deadline):
            return False
        signal.pidfd_send_signal(descriptor, signal.SIGKILL)
        return True
    except ProcessLookupError:
        return False
    finally:
        os.close(descriptor)


def drain(children):
    while True:
        try:
            if os.waitpid(-1, os.WNOHANG)[0]:
                continue
        except ChildProcessError:
            return
        for pid in children.read_text().split():
            signal_owned(int(pid))
        time.sleep(0.02)


def run():
    executable, home, workspace, output_root, timeout, lock = sys.argv[1:]
    os.set_inheritable(int(lock), False)
    if ctypes.CDLL(None, use_errno=True).prctl(36, 1, 0, 0, 0) != 0:
        raise OSError(ctypes.get_errno(), "cannot establish child subreaper")
    probe = os.pidfd_open(os.getpid())
    os.close(probe)
    children = Path("/proc/thread-self/children")
    children.read_text()
    instruction = sys.stdin.buffer.readline()
    if not instruction:
        return
    start = json.loads(instruction)
    with (tempfile.TemporaryDirectory(dir=output_root) as folder,
          tempfile.TemporaryFile(dir=folder) as prompt):
        output = Path(folder, "reply")
        prompt.write(start["prompt"].encode())
        prompt.seek(0)
        command = [
            executable, "exec", "--skip-git-repo-check", "--ephemeral",
            "--sandbox", "workspace-write", "-c", 'approval_policy="never"',
            "-c", 'shell_environment_policy.inherit="none"',
            "--output-last-message", str(output), "-",
        ]
        child_env = dict(os.environ, HOME=home, CODEX_HOME=home)
        lease = os.dup(sys.stdin.fileno())
        parent = select.poll()
        parent.register(lease, select.POLLHUP | select.POLLERR)
        try:
            require_spawn_authority(start["deadline"], parent)
            child = subprocess.Popen(
                command, cwd=workspace, env=child_env, stdin=prompt,
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True,
                preexec_fn=lambda: require_spawn_authority(start["deadline"], parent),
            )
        except (TimeoutError, subprocess.SubprocessError):
            emit("unknown")
            return
        finally:
            os.close(lease)
        cancelled = False
        expired = False
        try:
            emit("started")
            until = time.monotonic() + int(timeout)
            while child.poll() is None:
                if time.monotonic() >= until:
                    expired = True
                    break
                if not select.select([sys.stdin], [], [], 0.05)[0]:
                    continue
                instruction = sys.stdin.buffer.readline()
                if not instruction:
                    expired = True
                    break
                deadline = json.loads(instruction)["deadline"]
                if child.poll() is None:
                    cancelled = signal_owned(child.pid, deadline)
                    if cancelled:
                        break
        finally:
            try:
                if child.poll() is None:
                    signal_owned(child.pid)
                child.wait()
            finally:
                drain(children)
        if cancelled and child.returncode == -signal.SIGKILL:
            emit("cancelled")
        elif not expired and child.returncode == 0:
            text = read_reply(output)
            if text:
                emit("result", text=text)
            else:
                emit("unknown")
        else:
            emit("unknown")


def read_reply(output):
    try:
        descriptor = os.open(output, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
    except OSError:
        return ""
    with os.fdopen(descriptor, "rb") as reply:
        if not stat.S_ISREG(os.fstat(reply.fileno()).st_mode):
            return ""
        return reply.read(3800).decode("utf-8", errors="ignore").strip()


run()

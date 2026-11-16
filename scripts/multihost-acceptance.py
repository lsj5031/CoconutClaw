#!/usr/bin/env python3
"""Portable development launcher/control client. No login, installs or discovery."""
import argparse
import hashlib
import json
import os
import shutil
import socket
import struct
import subprocess
import sys
from pathlib import Path

ENTRY = "store::task_adapter::proposals::process::remote::multihost_role_entry"
INTEGRITY_ENTRY = "store::task_adapter::proposals::process::provider::host::integrity_diagnostic_entry"
MARKER = "CoconutClaw disposable multihost acceptance v1\n"


def dump(path, value):
    with path.open("x") as out:
        json.dump(value, out, indent=2)
        out.write("\n")


def root_check(root):
    if (root / "MULTIHOST").read_text() != MARKER:
        raise RuntimeError("Refuse unmarked existing fixture root")


def receive(sock, size):
    data = b""
    while len(data) < size:
        part = sock.recv(size - len(data))
        if not part:
            raise RuntimeError("Role closed control before acknowledgment, inspect role log")
        data += part
    return data


def main():
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest="action", required=True)
    b = sub.add_parser("build", help="Build the native dev-role executable from cached dependencies")
    b.add_argument("--output", type=Path, required=True)
    q = sub.add_parser("prepare", help="Create one fresh disposable role root/config")
    q.add_argument("--root", type=Path, required=True)
    q.add_argument("--role", choices=["coordinator", "runner"], required=True)
    q.add_argument("--run-id", required=True)
    q.add_argument("--bind", required=True, help="Explicit task UDP bind IP:0, no discovery")
    q.add_argument("--relay-url", help="Only an explicitly supplied reachable relay, no default")
    q.add_argument("--core-ready", type=Path, help="Runner pins this public coordinator ready file")
    q.add_argument("--provider-profile", type=Path, help="Runner-only nonsecret host profile JSON")
    l = sub.add_parser("launch", help="Launch one owned role, returns PID, then inspect ready.json/log")
    l.add_argument("--root", type=Path, required=True)
    l.add_argument("--executable", type=Path, required=True)
    i = sub.add_parser("integrity", help="Read-only BLAKE3 config/auth-metadata comparison, no provider invocation")
    i.add_argument("--root", type=Path, required=True)
    i.add_argument("--executable", type=Path, required=True)
    c = sub.add_parser("control", help="Bounded local coordinator control")
    c.add_argument("--root", type=Path, required=True)
    c.add_argument("command", choices=["run", "replay", "status", "cancel", "stop"])
    c.add_argument("--runner-ready", type=Path)
    c.add_argument("--allow-same-host", action="store_true", help="Local regression ONLY, never a multihost claim")
    a = p.parse_args()
    if a.action == "build":
        repo = Path(__file__).resolve().parents[1]
        if not a.output.is_absolute() or a.output.exists():
            raise RuntimeError("Output must be a new absolute executable path")
        # No online fetch/install fallback. Report cargo's named cache/tool need.
        result = subprocess.run(
            ["cargo", "test", "-p", "coconutclaw", "--bin", "coconutclaw",
             "--locked", "--offline", "--no-run", "--message-format=json"],
            cwd=repo, stdout=subprocess.PIPE, check=True,
        )
        targets = [
            v["executable"] for v in map(json.loads, result.stdout.decode().splitlines())
            if v.get("reason") == "compiler-artifact" and v.get("executable")
            and v["target"]["name"] == "coconutclaw" and v["profile"]["test"]
        ]
        if len(targets) != 1:
            raise RuntimeError("Expected exactly one native role harness artifact")
        a.output.parent.mkdir(parents=True, exist_ok=True)
        with a.output.open("xb") as out, open(targets[0], "rb") as source:
            shutil.copyfileobj(source, out)
        a.output.chmod(0o755)
        print(json.dumps({"executable": str(a.output), "sha256": hashlib.sha256(a.output.read_bytes()).hexdigest(),
                          "entry": ENTRY, "platform": sys.platform}))
    elif a.action == "integrity":
        if not a.root.is_absolute() or not a.executable.is_absolute():
            raise RuntimeError("Absolute fixture root and executable required")
        root_check(a.root)
        env = os.environ.copy()
        env["COCONUTCLAW_HOST_INTEGRITY_ROOT"] = str(a.root)
        result = subprocess.run(
            [str(a.executable), "--exact", INTEGRITY_ENTRY, "--ignored", "--nocapture"],
            env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        if result.returncode:
            raise RuntimeError("Read-only integrity diagnostic failed; no provider invoked")
        prefix = "COCONUTCLAW_INTEGRITY="
        # A serial libtest harness can print its progress label before stdout.
        records = [line.partition(prefix)[2] for line in result.stdout.decode().splitlines()
                   if prefix in line]
        if len(records) != 1:
            raise RuntimeError("Expected exactly one read-only integrity record")
        print(json.dumps(json.loads(records[0])))
    elif a.action == "prepare":
        if not a.root.is_absolute():
            raise RuntimeError("Absolute fresh root required")
        a.root.mkdir(mode=0o700)
        (a.root / "MULTIHOST").write_text(MARKER)
        core = json.loads(a.core_ready.read_text()) if a.core_ready else None
        if a.role == "runner" and (not core or core["run_id"] != a.run_id):
            raise RuntimeError("Runner requires matching public coordinator ready file")
        if a.role == "runner" and not a.provider_profile:
            raise RuntimeError("Real runner requires an explicit nonsecret provider profile, no mock fallback")
        profile = json.loads(a.provider_profile.read_text()) if a.provider_profile else None
        cfg = {"root": str(a.root), "role": a.role, "run_id": a.run_id,
               "network": {"bind": a.bind, "relay": a.relay_url},
               "owner": core["owner"] if core else None,
               "core": core["control"]["id"] if core else None, "provider": profile}
        dump(a.root / "role.json", cfg)
        print(json.dumps({"config": str(a.root / "role.json")}))
    elif a.action == "launch":
        root_check(a.root)
        if not a.executable.is_absolute() or not a.executable.is_file():
            raise RuntimeError("Explicit installed native role executable required")
        stopped = a.root / "stopped.json"
        if (a.root / "launcher.json").exists() and not stopped.exists():
            raise RuntimeError("Prior role not proven stopped, do not blindly restart or remove locks")
        # Ready is a live-process receipt; old receipts stay under numbered logs.
        count = len(list(a.root.glob("launch-*.json"))) + 1
        if (a.root / "ready.json").exists():
            os.rename(a.root / "ready.json", a.root / ("ready-before-%d.json" % count))
        if stopped.exists():
            os.rename(stopped, a.root / ("stopped-before-%d.json" % count))
        env = os.environ.copy()
        env["COCONUTCLAW_MULTIHOST_CONFIG"] = str(a.root / "role.json")
        with (a.root / ("role-%d.log" % count)).open("xb") as log:
            child = subprocess.Popen([str(a.executable), "--exact", ENTRY, "--ignored", "--nocapture"],
                                     env=env, cwd=a.root, stdin=subprocess.DEVNULL,
                                     stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        result = {"pid": child.pid, "ready": str(a.root / "ready.json"),
                  "log": str(a.root / ("role-%d.log" % count)),
                  "sha256": hashlib.sha256(a.executable.read_bytes()).hexdigest()}
        dump(a.root / ("launch-%d.json" % count), result)
        (a.root / "launcher.json").write_text(json.dumps(result) + "\n")
        print(json.dumps(result))
    else:
        root_check(a.root)
        message = {"command": a.command, "allow_same_host": a.allow_same_host}
        if a.runner_ready:
            message["runner_ready"] = json.loads(a.runner_ready.read_text())
        if a.command in ("run", "replay") and not a.runner_ready:
            raise RuntimeError("Run/replay requires the current public runner ready file")
        data = json.dumps(message).encode()
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
            sock.connect(str(a.root / "control.sock"))
            sock.sendall(struct.pack(">I", len(data)) + data)
            size = struct.unpack(">I", receive(sock, 4))[0]
            if not 0 < size <= 256 * 1024:
                raise RuntimeError("Invalid control response size")
            response = json.loads(receive(sock, size))
        print(json.dumps(response, indent=2))
        if response.get("ok") is False:
            raise SystemExit(1)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Offline official SANY/TLC gate. Never treats a budget exit as a proof."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import shutil
import subprocess
import time

ROOT = Path(__file__).resolve().parents[1]
JAR_HASH = "936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88"
JAVA = Path("/home/leo/.local/share/mise/installs/java/temurin-21.0.13+9.0.LTS/bin/java")
JAR = Path("/home/leo/.local/share/tlaplus/v1.7.4/tla2tools.jar")


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def classify(case, code, text, timed_out=False):
    stats = re.findall(
        r"([\d,]+) states generated, ([\d,]+) distinct states found, ([\d,]+) states left on queue",
        text,
    )
    counts = [int(s.replace(",", "")) for s in stats[-1]] if stats else None
    if timed_out:
        return False, "incomplete-time-budget", counts
    if any(error in text for error in ("OutOfMemoryError", "StackOverflowError", "GC overhead limit")):
        return False, "incomplete-resource-budget", counts
    if case.get("violation"):
        name = case["violation"]
        intended = (
            f"Invariant {name} is violated" in text
            if not case.get("liveness") and name != "AllSettled"
            else "Temporal properties were violated" in text
        )
        trace = ("The behavior up to this point is" in text
                 or "The following behavior constitutes a counter-example" in text)
        expected_exit = 13 if case.get("liveness") or name == "AllSettled" else 12
        ok = code == expected_exit and intended and trace
        return ok, "expected-counterexample" if ok else "wrong-or-missing-counterexample", counts
    ok = (
        code == 0
        and "Model checking completed. No error has been found." in text
        and counts is not None
        and counts[1] > 1
        and counts[2] == 0
        and "Finished in" in text
    )
    return ok, "exhausted-finite-state-space" if ok else "incomplete-or-failed", counts


def invoke(command, cwd, log, timeout):
    started = time.monotonic()
    timed_out = False
    with log.open("w", encoding="utf-8") as output:
        child = subprocess.Popen(
            command, cwd=cwd, stdout=output, stderr=subprocess.STDOUT,
            start_new_session=(os.name == "posix"),
        )
        try:
            code = child.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            timed_out = True
            if os.name == "posix":
                os.killpg(child.pid, signal.SIGKILL)
            else:
                child.kill()
            code = child.wait()
    return code, timed_out, time.monotonic() - started


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact-dir", required=True, type=Path)
    parser.add_argument("--java", type=Path, default=JAVA)
    parser.add_argument("--jar", type=Path, default=JAR)
    parser.add_argument("--cases", nargs="*")
    parser.add_argument("--timeout", type=int, default=90)
    args = parser.parse_args()
    if not args.artifact_dir.is_absolute():
        parser.error("--artifact-dir must be absolute and new")
    if not 1 <= args.timeout <= 180:
        parser.error("--timeout must be 1..180 seconds per owned JVM")
    if digest(args.jar) != JAR_HASH:
        parser.error("official v1.7.4 jar checksum mismatch")
    formal = ROOT / "formal"
    pins = json.loads((formal / "implementation-hashes.json").read_text())
    for path, expected in pins.items():
        if digest(ROOT / path) != expected:
            parser.error(f"stale implementation mapping: {path}")
    cases = json.loads((formal / "cases.json").read_text())
    mapping = json.loads((formal / "source-map.json").read_text())
    if set(mapping["families"]) != {p.stem for p in formal.glob("*.tla")}:
        parser.error("model/source-action family inventory mismatch")
    for family in mapping["families"].values():
        for anchors in family["actions"].values():
            for anchor in anchors:
                if pins.get(anchor["path"]) != anchor["sha256"]:
                    parser.error("source-action hash mismatch")
                lines = (ROOT / anchor["path"]).read_text().splitlines()
                if any(lines[d["line"] - 1].strip() != d["text"] for d in anchor["declarations"]):
                    parser.error("stale source-action declaration")
    if {p.name for p in formal.glob("*.cfg")} != {case["config"] for case in cases}:
        parser.error("configuration inventory mismatch")
    if len({c["id"] for c in cases}) != len(cases):
        parser.error("duplicate case identity")
    if args.cases:
        unknown = set(args.cases) - {case["id"] for case in cases}
        if unknown:
            parser.error(f"unknown cases: {sorted(unknown)}")
        cases = [c for c in cases if c["id"] in args.cases]
    args.artifact_dir.mkdir(parents=True, exist_ok=False)
    artifact = args.artifact_dir
    hashes = {
        str(p.relative_to(ROOT)): digest(p)
        for p in sorted(formal.rglob("*")) if p.is_file()
    }
    hashes["scripts/check-formal.py"] = digest(Path(__file__))
    for path in hashes:
        destination = artifact / "inputs" / path
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ROOT / path, destination)
    (artifact / "input-hashes.json").write_text(json.dumps(hashes, indent=2) + "\n")
    jvm = [str(args.java), "-Xms64m", "-Xmx512m", "-XX:ActiveProcessorCount=2", "-cp", str(args.jar)]
    report = {"jar_sha256": digest(args.jar), "java_sha256": digest(args.java),
              "heap_mib": 512, "workers": 1, "timeout_seconds": args.timeout,
              "implementation_files": len(pins), "sany": [], "cases": [], "complete": False}
    save = lambda: (artifact / "checks.json").write_text(json.dumps(report, indent=2) + "\n")
    save()
    for module in sorted({case["module"] for case in cases}):
        command = jvm + ["tla2sany.SANY", module + ".tla"]
        log = artifact / (module + "-sany.log")
        code, timeout, elapsed = invoke(command, formal, log, args.timeout)
        text = log.read_text()
        ok = code == 0 and not timeout and "*** Errors" not in text and (
            "Semantic processing of module " + module in text
        )
        report["sany"].append({"module": module, "command": command, "exit": code,
                               "seconds": elapsed, "accepted": ok, "log_sha256": digest(log)})
        save()
        if not ok:
            return 1
    for case in cases:
        log = artifact / (case["id"] + ".log")
        command = jvm + ["tlc2.TLC", "-workers", "1", "-fpmem", "0.2", "-fp", "0",
                         "-seed", "1", "-coverage", "1", "-metadir",
                         str(artifact / (case["id"] + "-states")), "-config",
                         str(formal / case["config"]), case["module"]]
        code, timeout, elapsed = invoke(command, formal, log, args.timeout)
        ok, classification, counts = classify(case, code, log.read_text(), timeout)
        coverage = {
            name: [int(a), int(b or a)]
            for name, a, b in re.findall(
                r"^<(\w+) line [^\n]*>: (\d+)(?::(\d+))?", log.read_text(), re.MULTILINE
            )
        }
        missing_actions = [
            action for action in case.get("required_actions", [])
            if action not in coverage or coverage[action][1] == 0
        ]
        if ok and missing_actions:
            ok, classification = False, "unexercised-required-actions"
        report["cases"].append({**case, "command": command, "exit": code, "timed_out": timeout,
                                "seconds": elapsed, "accepted": ok, "classification": classification,
                                "action_coverage": coverage, "unexercised_required_actions": missing_actions,
                                "states_generated_distinct_queue": counts, "log_sha256": digest(log)})
        save()
        print(case["id"], classification, counts, flush=True)
        if not ok:
            return 1
    if any(digest(ROOT / p) != v for p, v in hashes.items()):
        raise RuntimeError("formal inputs changed while checking")
    if any(digest(ROOT / p) != v for p, v in pins.items()):
        raise RuntimeError("implementation changed while checking")
    report["complete"] = True
    save()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

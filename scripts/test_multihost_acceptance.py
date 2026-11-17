"""Offline tests for the existing role launcher's new bounded controls."""
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

SCRIPT = Path(__file__).with_name("multihost-acceptance.py")
SHIM = SCRIPT.parents[1] / "crates/coconutclaw-cli/src/store/task_adapter/proposals/process/provider/host_auth_in_place.py"


class Controls(unittest.TestCase):
    def test_profile_projects_route_without_auth_command_arguments(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = root / "config.toml"
            config.write_text('model_provider="fixture"\n[model_providers.fixture]\n'
                              'base_url="http://127.0.0.1:1/v1"\n'
                              '[model_providers.fixture.auth]\ncommand="/bin/echo"\n'
                              'args=["synthetic-private-argument"]\n')
            auth = root / "auth.json"
            auth.write_text("unreadable-as-json-command-mode-does-not-need-key")
            output = root / "profile.json"
            before = config.read_bytes(), auth.read_bytes()
            result = subprocess.run([sys.executable, str(SCRIPT), "profile",
                                     "--output", str(output), "--executable", sys.executable,
                                     "--python", sys.executable, "--auth-json", str(auth),
                                     "--shared-config", str(config), "--controlled"],
                                    capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertTrue(json.loads(output.read_text())["auth_command_in_place"])
            self.assertNotIn("synthetic-private-argument", output.read_text() + result.stdout)
            self.assertEqual(before, (config.read_bytes(), auth.read_bytes()))

    def test_gate_requires_owned_matching_live_tool_and_releases_once(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "MULTIHOST").write_text("CoconutClaw disposable multihost acceptance v1\n")
            (root / "role.json").write_text(json.dumps(dict(
                role="runner", run_id="owned-fixture-123456", provider=dict(controlled_fixture=True))))
            workspace = root / "workspace/1"
            workspace.mkdir(parents=True)
            command = [sys.executable, str(SCRIPT), "fixture", "--root", str(root), "release"]
            self.assertNotEqual(subprocess.run(command, capture_output=True).returncode, 0)
            (workspace / "fixture-tool-started.json").write_text('{"nonce":"foreign"}')
            self.assertNotEqual(subprocess.run(command, capture_output=True).returncode, 0)
            (workspace / "fixture-tool-started.json").write_text('{"nonce":"owned-fixture-123456","tool_pid":123}')
            self.assertEqual(subprocess.run(command, capture_output=True).returncode, 0)
            original = (workspace / "fixture-release.json").read_bytes()
            self.assertEqual(subprocess.run(command, capture_output=True).returncode, 0)
            self.assertEqual(original, (workspace / "fixture-release.json").read_bytes())
            (workspace / "fixture-tool-ended.json").write_text('{"cancelled":true}')
            self.assertNotEqual(subprocess.run(command, capture_output=True).returncode, 0)

    def test_auth_callback_reads_original_command_context_in_place(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            command = root / "auth-command"
            command.write_text("#!/bin/sh\nprintf '%s' \"$CODEX_HOME\"\n")
            command.chmod(0o700)
            config = root / "config.toml"
            config.write_text('model_provider="fixture"\n[model_providers.fixture]\n'
                              'base_url="http://127.0.0.1:1/v1"\n'
                              '[model_providers.fixture.auth]\ncommand=' + json.dumps(str(command)) + "\n")
            profile = root / "profile.json"
            profile.write_text(json.dumps(dict(shared_config=str(config), provider_name="fixture",
                                               base_url="http://127.0.0.1:1/v1")))
            before = config.read_bytes(), config.stat().st_mtime_ns
            result = subprocess.run([sys.executable, "-I", str(SHIM), str(profile)],
                                    capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stdout, str(root))
            self.assertEqual(before, (config.read_bytes(), config.stat().st_mtime_ns))
            value = json.loads(profile.read_text())
            value["base_url"] = "http://127.0.0.1:2/foreign"
            profile.write_text(json.dumps(value))
            self.assertNotEqual(subprocess.run([sys.executable, "-I", str(SHIM), str(profile)],
                                              capture_output=True).returncode, 0)


if __name__ == "__main__":
    unittest.main()

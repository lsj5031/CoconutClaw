"""Native auth callback, original host configuration only; no token persistence."""
import json
import os
import sys
import tomllib
from pathlib import Path

cfg = json.loads(Path(sys.argv[1]).read_text())
shared = Path(cfg["shared_config"])
source = tomllib.loads(shared.read_text())
assert source["model_provider"] == cfg["provider_name"]
provider = source["model_providers"][cfg["provider_name"]]
assert provider["base_url"] == cfg["base_url"]
auth = provider["auth"]
command = Path(auth["command"])
args = auth.get("args", [])
assert command.is_absolute() and command.is_file()
assert isinstance(args, list) and all(isinstance(v, str) for v in args)
os.environ["CODEX_HOME"] = str(shared.parent)
os.execv(str(command), [str(command), *args])

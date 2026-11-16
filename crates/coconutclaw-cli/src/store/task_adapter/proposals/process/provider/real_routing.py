"""Generate non-secret routing only; the auth command reads original config in place."""
import json
import sys
import tomllib
from pathlib import Path
from urllib.parse import urlsplit

source = tomllib.loads(Path("/home/leo/.codex/config.toml").read_text())
name = source["model_provider"]
provider = source["model_providers"][name]
url = urlsplit(provider["base_url"])
assert not url.username and not url.password and not url.query and not url.fragment
assert isinstance(provider["auth"], dict) and "command" in provider["auth"]
home = Path(sys.argv[1])
lines = ["model_provider = " + json.dumps(name), "[model_providers." + json.dumps(name) + "]"]
for key in ("name", "base_url", "wire_api", "supports_websockets"):
    if key in provider:
        assert isinstance(provider[key], (str, bool))
        lines.append(key + " = " + json.dumps(provider[key]))
lines += [
    "[model_providers." + json.dumps(name) + ".auth]",
    'command = "/usr/bin/python3"',
    "args = " + json.dumps(["-I", str(home / "auth-in-place.py")]),
    "timeout_ms = " + str(int(provider["auth"].get("timeout_ms", 10000))),
]
with (home / "config.toml").open("x") as config:
    config.write("\n".join(lines) + "\n")

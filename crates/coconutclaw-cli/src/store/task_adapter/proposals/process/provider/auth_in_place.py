"""Forward native auth to the existing local command without copying credentials."""
import os
import tomllib
from pathlib import Path

config = tomllib.loads(Path("/home/leo/.codex/config.toml").read_text())
auth = config["model_providers"][config["model_provider"]]["auth"]
assert Path(auth["command"]).is_absolute() and Path(auth["command"]).is_file()
# Only this existing auth command gets its original context. Native Codex keeps
# its disposable CODEX_HOME; neither auth files nor returned tokens are copied.
os.environ["CODEX_HOME"] = "/home/leo/.codex"
os.execv(auth["command"], [auth["command"], *auth.get("args", [])])

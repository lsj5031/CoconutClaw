$ErrorActionPreference = "Stop"
$scriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
& "$scriptRoot\run.ps1" -Heartbeat @args
exit $LASTEXITCODE

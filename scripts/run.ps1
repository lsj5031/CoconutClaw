param(
    [switch]$Once,
    [switch]$Doctor,
    [switch]$Heartbeat,
    [switch]$NightlyReflection,
    [string]$InjectText,
    [string]$InjectFile,
    [string]$ChatId,
    [string]$InstanceDir = ".",
    [switch]$UseCargo
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

$command = "run"
if ($Doctor) {
    $command = "doctor"
} elseif ($Heartbeat) {
    $command = "heartbeat"
} elseif ($NightlyReflection) {
    $command = "nightly-reflection"
} elseif ($Once) {
    $command = "once"
}

$cliArgs = @("--instance-dir", $InstanceDir, $command)
if ($command -eq "once") {
    if ($InjectText) {
        $cliArgs += @("--inject-text", $InjectText)
    }
    if ($InjectFile) {
        $cliArgs += @("--inject-file", $InjectFile)
    }
    if ($ChatId) {
        $cliArgs += @("--chat-id", $ChatId)
    }
}

$bin = Get-Command coconutclaw -ErrorAction SilentlyContinue
if (-not $UseCargo -and $bin) {
    & $bin.Source @cliArgs
    exit $LASTEXITCODE
}

$releaseBin = Join-Path $repoRoot "target\release\coconutclaw.exe"
if (-not $UseCargo -and (Test-Path -LiteralPath $releaseBin)) {
    & $releaseBin @cliArgs
    exit $LASTEXITCODE
}

$debugBin = Join-Path $repoRoot "target\debug\coconutclaw.exe"
if (-not $UseCargo -and (Test-Path -LiteralPath $debugBin)) {
    & $debugBin @cliArgs
    exit $LASTEXITCODE
}

& cargo run -p coconutclaw -- @cliArgs
exit $LASTEXITCODE

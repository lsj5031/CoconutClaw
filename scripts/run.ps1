param(
    [switch]$Once,
    [switch]$Doctor,
    [switch]$Heartbeat,
    [switch]$NightlyReflection,
    [string]$InjectText,
    [string]$InjectFile,
    [string]$ChatId,
    [string]$Instance,
    [string]$InstanceDir = ".",
    [switch]$UseCargo
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

$instanceSpecified = $PSBoundParameters.ContainsKey("Instance")
$instanceDirSpecified = $PSBoundParameters.ContainsKey("InstanceDir")
if ($instanceSpecified -and $instanceDirSpecified) {
    throw "-Instance and -InstanceDir are mutually exclusive"
}

if ($instanceSpecified -and $Instance -notmatch "^[a-zA-Z0-9_.-]+$") {
    throw "invalid instance: $Instance (expected [a-zA-Z0-9_.-])"
}

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

if ($instanceSpecified) {
    $cliArgs = @("--instance", $Instance, $command)
} else {
    $cliArgs = @("--instance-dir", $InstanceDir, $command)
}
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

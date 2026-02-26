param(
    [Parameter(Mandatory = $true, Position = 0)]
    [ValidateSet("install", "start", "stop", "status", "uninstall")]
    [string]$Command,
    [string]$Instance,
    [string]$InstanceDir = ".",
    [ValidatePattern("^(?:[01][0-9]|2[0-3]):[0-5][0-9]$")]
    [string]$HeartbeatTime = "09:00",
    [ValidatePattern("^(?:[01][0-9]|2[0-3]):[0-5][0-9]$")]
    [string]$ReflectionTime = "22:30",
    [switch]$UseCargo
)

$ErrorActionPreference = "Stop"
if ($PSVersionTable.PSVersion.Major -ge 7) {
    $PSNativeCommandUseErrorActionPreference = $false
}

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

$runScript = Join-Path $repoRoot "scripts\run.ps1"
$psExe = (Get-Command powershell.exe -ErrorAction SilentlyContinue).Source
if (-not $psExe) {
    throw "powershell.exe not found in PATH"
}

function Convert-ToSafeId {
    param([string]$Value)
    $safe = [regex]::Replace($Value.ToLowerInvariant(), "[^a-z0-9_.-]+", "-").Trim("-")
    if ([string]::IsNullOrWhiteSpace($safe)) {
        return "instance"
    }
    return $safe
}

function Get-StringHash {
    param([string]$Value)
    $sha1 = [System.Security.Cryptography.SHA1]::Create()
    try {
        $bytes = [System.Text.Encoding]::UTF8.GetBytes($Value)
        $hash = $sha1.ComputeHash($bytes)
    } finally {
        $sha1.Dispose()
    }
    return (-join ($hash | ForEach-Object { $_.ToString("x2") })).Substring(0, 8)
}

function Normalize-Path {
    param([string]$PathValue)
    return [System.IO.Path]::GetFullPath($PathValue).TrimEnd('\', '/')
}

function Get-DefaultDataDir {
    if ($env:COCONUTCLAW_DATA_DIR) {
        if ([System.IO.Path]::IsPathRooted($env:COCONUTCLAW_DATA_DIR)) {
            return $env:COCONUTCLAW_DATA_DIR
        }
        return Join-Path (Get-Location).Path $env:COCONUTCLAW_DATA_DIR
    }
    if ($env:LOCALAPPDATA) {
        return Join-Path $env:LOCALAPPDATA "CoconutClaw"
    }
    return Join-Path (Get-Location).Path ".coconutclaw\state"
}

$instanceSpecified = $PSBoundParameters.ContainsKey("Instance")
$instanceDirSpecified = $PSBoundParameters.ContainsKey("InstanceDir")
if ($instanceSpecified -and $instanceDirSpecified) {
    throw "-Instance and -InstanceDir are mutually exclusive"
}
if ($instanceSpecified -and $Instance -notmatch "^[a-zA-Z0-9_.-]+$") {
    throw "invalid instance: $Instance (expected [a-zA-Z0-9_.-])"
}

$instanceAbs = if ($instanceSpecified) {
    Join-Path (Get-DefaultDataDir) $Instance
} elseif ([System.IO.Path]::IsPathRooted($InstanceDir)) {
    $InstanceDir
} else {
    Join-Path $repoRoot $InstanceDir
}
[System.IO.Directory]::CreateDirectory($instanceAbs) | Out-Null

$instanceKey = "default"
$repoRootNorm = Normalize-Path $repoRoot
$instanceAbsNorm = Normalize-Path $instanceAbs
if ($instanceSpecified) {
    $normalized = Convert-ToSafeId $Instance
    if ($normalized -ne "default") {
        $instanceKey = $normalized
    }
} elseif (-not [string]::Equals($instanceAbsNorm, $repoRootNorm, [System.StringComparison]::OrdinalIgnoreCase)) {
    $base = Convert-ToSafeId ([System.IO.Path]::GetFileName($instanceAbsNorm))
    $hash = Get-StringHash $instanceAbsNorm
    $instanceKey = "dir-$base-$hash"
}

if ($instanceKey -eq "default") {
    $taskRun = "CoconutClaw-Run"
    $taskHeartbeat = "CoconutClaw-Heartbeat"
    $taskReflection = "CoconutClaw-NightlyReflection"
} else {
    $taskRun = "CoconutClaw-Run-$instanceKey"
    $taskHeartbeat = "CoconutClaw-Heartbeat-$instanceKey"
    $taskReflection = "CoconutClaw-NightlyReflection-$instanceKey"
}
$allTasks = @($taskRun, $taskHeartbeat, $taskReflection)

function New-TaskCommand {
    param(
        [ValidateSet("run", "heartbeat", "reflection")]
        [string]$Mode
    )

    $parts = @(
        "-NoProfile",
        "-ExecutionPolicy", "Bypass",
        "-File", "`"$runScript`""
    )
    if ($instanceSpecified) {
        $parts += @("-Instance", "`"$Instance`"")
    } else {
        $parts += @("-InstanceDir", "`"$instanceAbs`"")
    }
    if ($UseCargo) {
        $parts += "-UseCargo"
    }
    switch ($Mode) {
        "heartbeat" { $parts += "-Heartbeat" }
        "reflection" { $parts += "-NightlyReflection" }
    }

    return "$psExe $($parts -join ' ')"
}

function New-TimeString {
    param([string]$Value)
    return "$Value:00"
}

function Ensure-ConfigFile {
    $cfg = Join-Path $instanceAbs "config.toml"
    if (Test-Path -LiteralPath $cfg) {
        return
    }
    $template = Join-Path $repoRoot "config.toml.example"
    if (Test-Path -LiteralPath $template) {
        Copy-Item -LiteralPath $template -Destination $cfg
        Write-Host "created $cfg from config.toml.example"
        return
    }
    throw "missing config.toml and template config.toml.example"
}

function Install-Tasks {
    Ensure-ConfigFile

    $runCmd = New-TaskCommand -Mode run
    $heartbeatCmd = New-TaskCommand -Mode heartbeat
    $reflectionCmd = New-TaskCommand -Mode reflection

    & schtasks /Create /TN $taskRun /TR $runCmd /SC ONLOGON /F /RL LIMITED | Out-Null
    & schtasks /Create /TN $taskHeartbeat /TR $heartbeatCmd /SC DAILY /ST (New-TimeString $HeartbeatTime) /F /RL LIMITED | Out-Null
    & schtasks /Create /TN $taskReflection /TR $reflectionCmd /SC DAILY /ST (New-TimeString $ReflectionTime) /F /RL LIMITED | Out-Null

    Write-Host "installed tasks:"
    $allTasks | ForEach-Object { Write-Host "  $_" }
    Write-Host "instance key: $instanceKey"
    Write-Host "next: run .\scripts\start.ps1"
}

function Invoke-ForTasks {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Operation
    )
    foreach ($task in $allTasks) {
        $oldPreference = $ErrorActionPreference
        try {
            $ErrorActionPreference = "Continue"
            & schtasks $Operation /TN $task 2>$null | Out-Null
        } finally {
            $ErrorActionPreference = $oldPreference
        }
        if ($LASTEXITCODE -ne 0) {
            if ($Operation -eq "/Query" -or $Operation -eq "/Delete") {
                continue
            }
            Write-Warning "failed $Operation for task $task"
        }
    }
}

function Start-Tasks {
    Invoke-ForTasks -Operation "/Run"
}

function Stop-Tasks {
    Invoke-ForTasks -Operation "/End"
}

function Show-Status {
    foreach ($task in $allTasks) {
        Write-Host "----- $task -----"
        $oldPreference = $ErrorActionPreference
        try {
            $ErrorActionPreference = "Continue"
            & schtasks /Query /TN $task /FO LIST /V 2>$null
        } finally {
            $ErrorActionPreference = $oldPreference
        }
        if ($LASTEXITCODE -ne 0) {
            Write-Host "not installed"
        }
        Write-Host ""
    }
}

function Uninstall-Tasks {
    Stop-Tasks
    foreach ($task in $allTasks) {
        & schtasks /Delete /TN $task /F 2>$null | Out-Null
    }
}

switch ($Command) {
    "install" { Install-Tasks }
    "start" { Start-Tasks }
    "stop" { Stop-Tasks }
    "status" { Show-Status }
    "uninstall" { Uninstall-Tasks }
}

param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$ForwardArgs
)

$ErrorActionPreference = "Stop"
$scriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
& "$scriptRoot\service.ps1" uninstall @ForwardArgs
exit $LASTEXITCODE

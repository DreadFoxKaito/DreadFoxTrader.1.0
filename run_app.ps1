[CmdletBinding()]
param(
    [string]$BindHost = $(if ($env:CRYPTID_HOST) { $env:CRYPTID_HOST } else { "0.0.0.0" }),
    [int]$Port = $(if ($env:CRYPTID_PORT) { [int]$env:CRYPTID_PORT } else { 8000 }),
    [string]$VenvDir = $(if ($env:VENV_DIR) { $env:VENV_DIR } else { ".venv" })
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
if (-not [System.IO.Path]::IsPathRooted($VenvDir)) {
    $VenvDir = Join-Path $Root $VenvDir
}

$PythonExe = Join-Path $VenvDir "Scripts\python.exe"
if (-not (Test-Path $PythonExe)) {
    Write-Error "Python executable not found: $PythonExe. Run install_windows.bat first."
    exit 1
}

Set-Location $Root
& $PythonExe -m app.main --http --host $BindHost --port $Port
exit $LASTEXITCODE

[CmdletBinding()]
param(
    [string]$BindHost = $(if ($env:CRYPTID_HOST) { $env:CRYPTID_HOST } else { "0.0.0.0" }),
    [int]$Port = $(if ($env:CRYPTID_PORT) { [int]$env:CRYPTID_PORT } else { 8000 }),
    [string]$VenvDir = $(if ($env:VENV_DIR) { $env:VENV_DIR } else { ".venv" }),
    [switch]$NoInitDb,
    [switch]$CleanRuntime,
    [switch]$WithAi,
    [switch]$WithSound,
    [switch]$WithStrategy,
    [switch]$Start
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $Root

if (-not [System.IO.Path]::IsPathRooted($VenvDir)) {
    $VenvDir = Join-Path $Root $VenvDir
}

$script:PythonExe = $null
$script:PythonBaseArgs = @()

function Write-Step {
    param([string]$Message)
    Write-Host "[install] $Message"
}

function Test-PythonCandidate {
    param(
        [string]$Exe,
        [string[]]$BaseArgs
    )

    $versionArgs = @()
    $versionArgs += $BaseArgs
    $versionArgs += "--version"
    & $Exe @versionArgs *> $null
    return ($LASTEXITCODE -eq 0)
}

function Set-PythonCandidate {
    param(
        [string]$Exe,
        [string[]]$BaseArgs
    )

    if (Test-PythonCandidate -Exe $Exe -BaseArgs $BaseArgs) {
        $script:PythonExe = $Exe
        $script:PythonBaseArgs = $BaseArgs
        return $true
    }
    return $false
}

function Resolve-Python {
    if ($env:PYTHON_BIN) {
        if (Set-PythonCandidate -Exe $env:PYTHON_BIN -BaseArgs @()) {
            return
        }
        throw "PYTHON_BIN is set but is not runnable: $env:PYTHON_BIN"
    }

    $pyLauncher = Get-Command py -ErrorAction SilentlyContinue
    if ($pyLauncher -and (Set-PythonCandidate -Exe $pyLauncher.Source -BaseArgs @("-3"))) {
        return
    }

    $python = Get-Command python -ErrorAction SilentlyContinue
    if ($python -and (Set-PythonCandidate -Exe $python.Source -BaseArgs @())) {
        return
    }

    $python3 = Get-Command python3 -ErrorAction SilentlyContinue
    if ($python3 -and (Set-PythonCandidate -Exe $python3.Source -BaseArgs @())) {
        return
    }

    throw @"
Python 3 was not found.

Install Python 3.11 or newer from https://www.python.org/downloads/windows/
or run:

  winget install -e --id Python.Python.3.13

Then open a new terminal and rerun install_windows.bat.
"@
}

function Invoke-BasePython {
    param([string[]]$PythonArgs)

    $allArgs = @()
    $allArgs += $script:PythonBaseArgs
    $allArgs += $PythonArgs
    & $script:PythonExe @allArgs
    if ($LASTEXITCODE -ne 0) {
        throw "Python command failed: $script:PythonExe $($allArgs -join ' ')"
    }
}

function Invoke-VenvPython {
    param([string[]]$PythonArgs)

    $venvPython = Join-Path $VenvDir "Scripts\python.exe"
    & $venvPython @PythonArgs
    if ($LASTEXITCODE -ne 0) {
        throw "Virtualenv Python command failed: $venvPython $($PythonArgs -join ' ')"
    }
}

Resolve-Python

if (-not (Test-Path (Join-Path $VenvDir "Scripts\python.exe"))) {
    Write-Step "Creating virtual environment at $VenvDir."
    Invoke-BasePython -PythonArgs @("-m", "venv", $VenvDir)
}
else {
    Write-Step "Using existing virtual environment at $VenvDir."
}

Write-Step "Installing Python requirements."
Invoke-VenvPython -PythonArgs @("-m", "pip", "install", "--upgrade", "pip", "setuptools", "wheel")
Invoke-VenvPython -PythonArgs @("-m", "pip", "install", "-r", (Join-Path $Root "requirements.txt"))

if ($WithAi) {
    Invoke-VenvPython -PythonArgs @("-m", "pip", "install", "-r", (Join-Path $Root "requirements-ai.txt"))
}
if ($WithSound) {
    Invoke-VenvPython -PythonArgs @("-m", "pip", "install", "-r", (Join-Path $Root "requirements-sound.txt"))
}
if ($WithStrategy) {
    Invoke-VenvPython -PythonArgs @("-m", "pip", "install", "-r", (Join-Path $Root "requirements-strategy.txt"))
}

$setupArgs = @((Join-Path $Root "setup_new_user.py"))
if ($CleanRuntime) {
    $setupArgs += "--clean-runtime"
    $setupArgs += "--yes"
}
if (-not $NoInitDb) {
    $setupArgs += "--init-db"
}

Write-Step "Initializing local config and runtime database."
Invoke-VenvPython -PythonArgs $setupArgs

Write-Host ""
Write-Step "Installation complete."
Write-Host ""
Write-Host "Start command:"
Write-Host "  .\run_windows.bat"
Write-Host ""
Write-Host "Local URL: http://127.0.0.1:$Port"
Write-Host "LAN URL:   http://<this-device-ip>:$Port"
Write-Host ""

if ($Start) {
    & (Join-Path $Root "run_app.ps1") -BindHost $BindHost -Port $Port -VenvDir $VenvDir
    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }
}

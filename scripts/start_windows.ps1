# Start FinAlly in Docker (Windows PowerShell). Idempotent: safe to run repeatedly.
#
# Usage: .\scripts\start_windows.ps1 [-Build] [-NoOpen]
#   -Build   force a rebuild of the image
#   -NoOpen  don't open the browser
param(
    [switch]$Build,
    [switch]$NoOpen
)

# Native docker commands write expected "not found" messages to stderr; check $LASTEXITCODE
# instead of letting PowerShell 5.1 turn that stderr into terminating errors.
$ErrorActionPreference = 'Continue'

$Image = 'finally'
$Container = 'finally'
$Volume = 'finally-data'
$Port = if ($env:FINALLY_PORT) { $env:FINALLY_PORT } else { '8000' }
$Url = "http://localhost:$Port"

$RootDir = Split-Path -Parent $PSScriptRoot
$EnvFile = Join-Path $RootDir '.env'

docker info *> $null
if ($LASTEXITCODE -ne 0) {
    Write-Error 'Docker is not running. Start Docker Desktop and try again.'
    exit 1
}

docker image inspect $Image *> $null
if ($Build -or $LASTEXITCODE -ne 0) {
    Write-Host "Building image '$Image'..."
    docker build -t $Image $RootDir
    if ($LASTEXITCODE -ne 0) { Write-Error 'Docker build failed.'; exit 1 }
}

# Replace any existing container (running or stopped) so the latest image is used.
docker container inspect $Container *> $null
if ($LASTEXITCODE -eq 0) {
    Write-Host "Removing existing container '$Container'..."
    docker rm -f $Container *> $null
}

$RunArgs = @('run', '-d', '--name', $Container, '-v', "${Volume}:/app/db", '-p', "${Port}:8000")
if (Test-Path $EnvFile) {
    $RunArgs += @('--env-file', $EnvFile)
} else {
    Write-Warning "$EnvFile not found. Continuing without it (AI chat needs OPENROUTER_API_KEY; see .env.example)."
}
$RunArgs += $Image

Write-Host "Starting container '$Container'..."
docker @RunArgs | Out-Null
if ($LASTEXITCODE -ne 0) { Write-Error 'Failed to start container.'; exit 1 }

Write-Host -NoNewline 'Waiting for FinAlly to become healthy'
for ($i = 0; $i -lt 30; $i++) {
    try {
        $resp = Invoke-WebRequest -Uri "$Url/api/health" -UseBasicParsing -TimeoutSec 2
        if ($resp.StatusCode -eq 200) { Write-Host ' ok'; break }
    } catch {
        Write-Host -NoNewline '.'
        Start-Sleep -Seconds 1
    }
}

Write-Host "FinAlly is running at $Url"
Write-Host 'Stop it with: .\scripts\stop_windows.ps1'

if (-not $NoOpen) {
    Start-Process $Url
}

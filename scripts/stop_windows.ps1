# Stop FinAlly (Windows PowerShell). Removes the container but keeps the finally-data volume.
$Container = 'finally'

docker container inspect $Container *> $null
if ($LASTEXITCODE -eq 0) {
    docker rm -f $Container *> $null
    Write-Host "FinAlly stopped. Data is kept in the 'finally-data' volume."
} else {
    Write-Host 'FinAlly is not running.'
}

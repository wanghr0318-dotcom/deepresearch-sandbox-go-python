[CmdletBinding()]
param(
    [Parameter(Mandatory)][string]$GpuHostname,
    [Parameter(Mandatory)][int]$GpuSshPort,
    [Parameter(Mandatory)][string]$GpuKey,
    [Parameter(Mandatory)][string]$CvmHostname,
    [Parameter(Mandatory)][string]$CvmKey,
    [string]$GpuUser = 'root',
    [string]$CvmUser = 'ubuntu',
    [int]$LocalPort = 33561,
    [int]$CvmPort = 33562
)

$ErrorActionPreference = 'Stop'
$ssh = (Get-Command ssh.exe).Source
$options = @(
    '-o', 'BatchMode=yes',
    '-o', 'StrictHostKeyChecking=yes',
    '-o', 'ConnectTimeout=12',
    '-o', 'ExitOnForwardFailure=yes',
    '-o', 'ServerAliveInterval=15',
    '-o', 'ServerAliveCountMax=3'
)
$model = 'Qwen/Qwen3-8B'
$gpu = $null
$cvm = $null

function Wait-Model {
    param([System.Diagnostics.Process]$Process, [scriptblock]$ReadModels)
    for ($attempt = 0; $attempt -lt 15; $attempt++) {
        if ($Process.HasExited) {
            throw "SSH forwarding exited with code $($Process.ExitCode)."
        }
        try {
            $response = & $ReadModels
            if ($model -in @($response.data.id)) {
                return
            }
            throw "Forward did not return $model."
        } catch {
            if ($attempt -eq 14) { throw }
        }
        Start-Sleep -Seconds 1
    }
}

try {
    $gpu = Start-Process -FilePath $ssh -PassThru -WindowStyle Hidden -ArgumentList (
        $options + @('-N', '-T', '-i', ('"{0}"' -f $GpuKey), '-p', $GpuSshPort,
            '-L', "127.0.0.1:${LocalPort}:127.0.0.1:30000", "$GpuUser@$GpuHostname")
    )
    Wait-Model $gpu {
        Invoke-RestMethod -Uri "http://127.0.0.1:$LocalPort/v1/models" -TimeoutSec 3
    }

    $cvm = Start-Process -FilePath $ssh -PassThru -WindowStyle Hidden -ArgumentList (
        $options + @('-N', '-T', '-i', ('"{0}"' -f $CvmKey),
            '-R', "127.0.0.1:${CvmPort}:127.0.0.1:$LocalPort", "$CvmUser@$CvmHostname")
    )
    Wait-Model $cvm {
        $models = & $ssh @options '-i' $CvmKey "$CvmUser@$CvmHostname" (
            "curl --max-time 4 -fsS http://127.0.0.1:$CvmPort/v1/models"
        )
        if ($LASTEXITCODE -ne 0) { throw 'CVM could not reach the model through the SSH bridge.' }
        $models | ConvertFrom-Json
    }
    Write-Output "CVM tunnel ready: http://127.0.0.1:$CvmPort/v1."
    while (!$gpu.HasExited -and !$cvm.HasExited) {
        Start-Sleep -Seconds 2
    }
    throw 'An SSH forwarding leg exited.'
} finally {
    foreach ($child in @($gpu, $cvm)) {
        if ($child -and !$child.HasExited) {
            Stop-Process -InputObject $child
        }
    }
}

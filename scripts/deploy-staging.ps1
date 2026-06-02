[CmdletBinding(SupportsShouldProcess = $true)]
param(
    [string]$SourceRef = "origin/main",
    [string]$RemoteName = "origin",
    [string]$StagingBranch = "staging",
    [switch]$AllowDirty
)

$ErrorActionPreference = "Stop"

function Invoke-Git {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$Arguments
    )

    & git @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "git $($Arguments -join ' ') failed with exit code $LASTEXITCODE"
    }
}

function Get-GitOutput {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$Arguments
    )

    $output = & git @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "git $($Arguments -join ' ') failed with exit code $LASTEXITCODE"
    }

    return $output
}

$repoRoot = Get-GitOutput @("rev-parse", "--show-toplevel")
Set-Location $repoRoot

$status = Get-GitOutput @("status", "--porcelain")
if ($status -and -not $AllowDirty) {
    throw "Working tree has uncommitted changes. Commit/stash them or rerun with -AllowDirty."
}

Invoke-Git @("fetch", $RemoteName, "--prune")

$sourceSha = (Get-GitOutput @("rev-parse", "$SourceRef^{commit}")).Trim()
$remoteStagingRef = "refs/heads/$StagingBranch"
$remoteStagingOutput = Get-GitOutput @("ls-remote", $RemoteName, $remoteStagingRef)
$remoteStagingSha = $remoteStagingOutput | Select-Object -First 1 | ForEach-Object { ($_ -split "`t")[0] }

Write-Host "Repository: $repoRoot"
Write-Host "Source ref: $SourceRef"
Write-Host "Source SHA: $sourceSha"
if ($remoteStagingSha) {
    Write-Host "Current remote $StagingBranch SHA: $remoteStagingSha"
} else {
    Write-Host "Remote branch $StagingBranch does not exist and will be created."
}

$pushRef = "$sourceSha`:$remoteStagingRef"
$lease = if ($remoteStagingSha) { "--force-with-lease=$remoteStagingRef`:$remoteStagingSha" } else { "--force-with-lease=$remoteStagingRef`:" }
if ($PSCmdlet.ShouldProcess("$RemoteName/$StagingBranch", "Promote $sourceSha from $SourceRef")) {
    Invoke-Git @("push", $lease, $RemoteName, $pushRef)
    Write-Host "Staging promotion pushed: $RemoteName/$StagingBranch -> $sourceSha"
    Write-Host "GitHub Actions should now run the staging deployment workflow."
}

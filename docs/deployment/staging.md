# Staging Deployment

This repository uses a lightweight staging deployment model because it currently contains documentation and no application runtime.

## What Staging Means

Staging is the remote `staging` branch plus a GitHub deployment record in the `staging` environment. Promoting to staging verifies the repository content and records the deployed commit in GitHub.

## Deploy Current Main to Staging

From a clean working tree:

```powershell
.\scripts\deploy-staging.ps1
```

The script promotes `origin/main` to `origin/staging`. Pushing `origin/staging` triggers `.github/workflows/staging-deploy.yml`.

## Deploy a Specific Ref

```powershell
.\scripts\deploy-staging.ps1 -SourceRef origin/main
```

Use a full commit SHA when promoting an exact revision:

```powershell
.\scripts\deploy-staging.ps1 -SourceRef 073fa4814b1833c9a74ec7c04a75ca05fad25e3a
```

## Dry Run

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\deploy-staging.ps1 -WhatIf -AllowDirty
```

Use `-AllowDirty` when testing from a dirty working tree. Without `-AllowDirty`, the script enforces a clean working tree before it reaches the `WhatIf` dry-run behavior.

## Verify Deployment

Check the remote branch:

```powershell
git ls-remote --heads origin staging
```

Check GitHub deployment records:

```powershell
Invoke-RestMethod -Uri "https://api.github.com/repos/w00125533/data-benchmark/deployments?environment=staging&per_page=5"
```

Check the latest staging deployment status:

```powershell
$deployment = Invoke-RestMethod -Uri "https://api.github.com/repos/w00125533/data-benchmark/deployments?environment=staging&per_page=1"
$latestStatus = Invoke-RestMethod -Uri $deployment[0].statuses_url | Select-Object -First 1

if ($latestStatus.state -ne "success") {
    throw "Latest staging deployment status is '$($latestStatus.state)', expected 'success'."
}

$latestStatus | Select-Object state,created_at,description
```

The command throws unless the latest status is `success`.

## Roll Back Staging

Promote a previous commit SHA:

```powershell
.\scripts\deploy-staging.ps1 -SourceRef <previous-staging-commit-sha>
```

The push uses `--force-with-lease`, so it refuses unsafe remote overwrites.

## Verify Generated Web Report

After packaging the runner, generate a local smoke report:

```powershell
java -jar target/data-benchmark-0.1.0-SNAPSHOT.jar run --mode local --config configs/benchmark-smoke.yml --run-id staging-web-report-smoke
```

Verify the standalone report package:

```powershell
Test-Path reports/runs/staging-web-report-smoke/index.html
Test-Path reports/runs/staging-web-report-smoke/report.json
```

Open `reports/runs/staging-web-report-smoke/index.html` directly in a browser. The removed monitoring stack is not part of the report viewing path.

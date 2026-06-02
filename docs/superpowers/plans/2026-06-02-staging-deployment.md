# Staging Deployment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a repeatable staging deployment path for this repository so `main` can be promoted to a `staging` branch, verified, and recorded as a GitHub `staging` deployment.

**Architecture:** This repository currently contains documentation only, so the first staging implementation should be branch- and GitHub Deployment API based rather than pretending there is an application runtime. A local PowerShell script promotes a chosen commit to `staging`; a GitHub Actions workflow runs repository checks and creates a deployment record for the `staging` environment.

**Tech Stack:** Git, PowerShell 5+, GitHub Actions, GitHub REST API via `actions/github-script`.

---

## Current Repository Facts

- Local branch is `main` and matches `origin/main`.
- Remote is `https://github.com/w00125533/data-benchmark.git`.
- Remote branches currently include only `main`.
- GitHub environments API currently returns zero environments.
- GitHub deployments API currently returns zero deployments.
- The checked-out tree contains documentation under `docs/`; there is no build tool, deploy script, Docker file, app runtime, or `.github/workflows` directory.

## File Structure

- Create: `.github/workflows/staging-deploy.yml`
  - Runs on pushes to `staging` and manual dispatch.
  - Validates that required documentation exists.
  - Creates a GitHub deployment record in the `staging` environment.
  - Marks the deployment `success` only after checks pass.

- Create: `scripts/deploy-staging.ps1`
  - Promotes a source ref, default `origin/main`, to the remote `staging` branch.
  - Refuses to run with uncommitted changes unless `-AllowDirty` is passed.
  - Fetches remote refs before promotion.
  - Pushes with an explicit `--force-with-lease=<ref>:<expected>` so accidental overwrites are guarded.

- Create: `docs/deployment/staging.md`
  - Documents the staging deployment model, commands, rollback process, and verification steps.

- Modify: `README.md`
  - Adds a short staging deployment entry point for future maintainers.

---

### Task 1: Add Local Staging Promotion Script

**Files:**
- Create: `scripts/deploy-staging.ps1`
- Test: manual script dry run commands in this task

- [ ] **Step 1: Create the script directory**

Run:

```powershell
New-Item -ItemType Directory -Force scripts
```

Expected:

```text
Directory: D:\agent-code\data-benchmark
Mode                 LastWriteTime         Length Name
----                 -------------         ------ ----
d-----                                      scripts
```

- [ ] **Step 2: Add `scripts/deploy-staging.ps1`**

Create `scripts/deploy-staging.ps1` with this complete content:

```powershell
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
```

- [ ] **Step 3: Verify the script parses**

Run:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\deploy-staging.ps1 -WhatIf
```

Expected:

```text
Repository: D:\agent-code\data-benchmark
Source ref: origin/main
Source SHA: 073fa4814b1833c9a74ec7c04a75ca05fad25e3a
Remote branch staging does not exist and will be created.
What if: Performing the operation "Promote 073fa4814b1833c9a74ec7c04a75ca05fad25e3a from origin/main" on target "origin/staging".
```

The SHA may differ if `main` has advanced.

- [ ] **Step 4: Commit the script**

Run:

```powershell
git add scripts/deploy-staging.ps1
git commit -m "chore: add staging promotion script"
```

Expected:

```text
[main <sha>] chore: add staging promotion script
 1 file changed
 create mode 100644 scripts/deploy-staging.ps1
```

---

### Task 2: Add GitHub Staging Deployment Workflow

**Files:**
- Create: `.github/workflows/staging-deploy.yml`
- Test: workflow syntax inspection and repository checks

- [ ] **Step 1: Create the workflow directory**

Run:

```powershell
New-Item -ItemType Directory -Force .github\workflows
```

Expected:

```text
Directory: D:\agent-code\data-benchmark\.github
Mode                 LastWriteTime         Length Name
----                 -------------         ------ ----
d-----                                      workflows
```

- [ ] **Step 2: Add `.github/workflows/staging-deploy.yml`**

Create `.github/workflows/staging-deploy.yml` with this complete content:

```yaml
name: Deploy to Staging

"on":
  push:
    branches:
      - staging
  workflow_dispatch:
    inputs:
      ref:
        description: Git ref to deploy to staging
        required: true
        default: main
        type: string

permissions:
  contents: read
  deployments: write

concurrency:
  group: staging-deployment
  cancel-in-progress: false

jobs:
  deploy:
    name: Record staging deployment
    runs-on: ubuntu-latest
    environment:
      name: staging
      deployment: false
    steps:
      - name: Checkout requested ref
        uses: actions/checkout@v4
        with:
          ref: ${{ github.event.inputs.ref || github.ref }}

      - name: Validate repository contents
        shell: bash
        run: |
          set -euo pipefail
          test -f docs/superpowers/specs/2026-06-02-starrocks-iceberg-benchmark-design.md
          test -s docs/superpowers/specs/2026-06-02-starrocks-iceberg-benchmark-design.md
          echo "Required benchmark design spec exists and is non-empty."

      - name: Create GitHub deployment
        id: deployment
        uses: actions/github-script@v7
        with:
          script: |
            const ref = context.payload.inputs?.ref || context.ref;
            const deployment = await github.rest.repos.createDeployment({
              owner: context.repo.owner,
              repo: context.repo.repo,
              ref,
              environment: 'staging',
              description: 'Staging deployment for data-benchmark',
              auto_merge: false,
              required_contexts: []
            });
            core.setOutput('deployment_id', String(deployment.data.id));

      - name: Mark staging deployment successful
        uses: actions/github-script@v7
        with:
          script: |
            const deployment_id = Number('${{ steps.deployment.outputs.deployment_id }}');
            await github.rest.repos.createDeploymentStatus({
              owner: context.repo.owner,
              repo: context.repo.repo,
              deployment_id,
              state: 'success',
              environment: 'staging',
              description: 'Repository checks passed for staging.'
            });
```

- [ ] **Step 3: Verify workflow file exists and contains required triggers**

Run:

```powershell
Select-String -Path .github\workflows\staging-deploy.yml -Pattern "push:","workflow_dispatch:","deployments: write","environment:","staging"
```

Expected output includes these lines:

```text
push:
workflow_dispatch:
deployments: write
environment:
name: staging
environment: 'staging'
```

- [ ] **Step 4: Commit the workflow**

Run:

```powershell
git add .github/workflows/staging-deploy.yml
git commit -m "ci: add staging deployment workflow"
```

Expected:

```text
[main <sha>] ci: add staging deployment workflow
 1 file changed
 create mode 100644 .github/workflows/staging-deploy.yml
```

---

### Task 3: Document Staging Operations

**Files:**
- Create: `docs/deployment/staging.md`
- Modify: `README.md`
- Test: documentation link checks with PowerShell

- [ ] **Step 1: Create the deployment docs directory**

Run:

```powershell
New-Item -ItemType Directory -Force docs\deployment
```

Expected:

```text
Directory: D:\agent-code\data-benchmark\docs
Mode                 LastWriteTime         Length Name
----                 -------------         ------ ----
d-----                                      deployment
```

- [ ] **Step 2: Add `docs/deployment/staging.md`**

Create `docs/deployment/staging.md` with this complete content:

```markdown
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

The push uses an explicit `--force-with-lease`, so it refuses unsafe remote overwrites.
```

- [ ] **Step 3: Add `README.md` if it does not exist**

Run:

```powershell
if (-not (Test-Path README.md)) {
    @"
# data-benchmark

Benchmark design and deployment assets for the StarRocks and Iceberg data benchmark project.

## Staging Deployment

See [docs/deployment/staging.md](docs/deployment/staging.md) for the staging deployment workflow.
"@ | Set-Content -Path README.md -Encoding utf8
}
```

Expected when `README.md` is missing:

```text
README.md is created with a Staging Deployment section.
```

Expected when `README.md` already exists:

```text
No output. Add the Staging Deployment section manually in the next step instead.
```

- [ ] **Step 4: If `README.md` already existed, add this section**

Append this section to `README.md` only if it is not already present:

```markdown
## Staging Deployment

See [docs/deployment/staging.md](docs/deployment/staging.md) for the staging deployment workflow.
```

- [ ] **Step 5: Verify documentation links**

Run:

```powershell
Test-Path docs\deployment\staging.md
Select-String -Path README.md -Pattern "docs/deployment/staging.md"
```

Expected:

```text
True
README.md:<line>:See [docs/deployment/staging.md](docs/deployment/staging.md) for the staging deployment workflow.
```

- [ ] **Step 6: Commit docs**

Run:

```powershell
git add README.md docs/deployment/staging.md
git commit -m "docs: document staging deployment"
```

Expected:

```text
[main <sha>] docs: document staging deployment
 2 files changed
 create mode 100644 README.md
 create mode 100644 docs/deployment/staging.md
```

If `README.md` already existed, the commit output should show `1 file changed` or `2 files changed` depending on whether the README needed edits.

---

### Task 4: Verify and Promote to Staging

**Files:**
- Uses: `scripts/deploy-staging.ps1`
- Uses: `.github/workflows/staging-deploy.yml`
- Uses: `docs/deployment/staging.md`

- [ ] **Step 1: Verify local git state**

Run:

```powershell
git status --short --branch
```

Expected:

```text
## main...origin/main [ahead 3]
```

The branch may be ahead by a different number if tasks were squashed.

- [ ] **Step 2: Push `main`**

Run:

```powershell
git push origin main
```

Expected:

```text
To https://github.com/w00125533/data-benchmark.git
   <old-sha>..<new-sha>  main -> main
```

- [ ] **Step 3: Dry-run staging promotion**

Run:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\deploy-staging.ps1 -WhatIf
```

Expected:

```text
Repository: D:\agent-code\data-benchmark
Source ref: origin/main
Source SHA: <new-main-sha>
Remote branch staging does not exist and will be created.
What if: Performing the operation "Promote <new-main-sha> from origin/main" on target "origin/staging".
```

If `staging` already exists, expected output includes:

```text
Current remote staging SHA: <existing-staging-sha>
```

- [ ] **Step 4: Promote to staging**

Run:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\deploy-staging.ps1
```

Expected:

```text
Staging promotion pushed: origin/staging -> <new-main-sha>
GitHub Actions should now run the staging deployment workflow.
```

- [ ] **Step 5: Verify remote staging branch**

Run:

```powershell
git ls-remote --heads origin staging
```

Expected:

```text
<new-main-sha>	refs/heads/staging
```

- [ ] **Step 6: Verify GitHub deployment record**

Run:

```powershell
Invoke-RestMethod -Uri "https://api.github.com/repos/w00125533/data-benchmark/deployments?environment=staging&per_page=1" | ConvertTo-Json -Depth 5
```

Expected:

```json
{
  "value": [
    {
      "environment": "staging",
      "description": "Staging deployment for data-benchmark"
    }
  ],
  "Count": 1
}
```

PowerShell may include additional deployment fields such as `id`, `sha`, `ref`, `creator`, and `statuses_url`.

---

## Self-Review

- Spec coverage: The observed deployment gap is covered by a local promotion script, a remote `staging` branch workflow, GitHub deployment status recording, and operator documentation.
- Placeholder scan: The implementation plan avoids `TBD`, `TODO`, `implement later`, and undefined "appropriate" work. The only angle-bracket values are command output variables and user-supplied commit SHAs in operational docs.
- Type consistency: The branch name is consistently `staging`, the environment name is consistently `staging`, the promotion script parameter is consistently `SourceRef`, and workflow deployment records use the same repository name.

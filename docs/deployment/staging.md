# Staging Deployment

This repository uses a lightweight staging deployment model because it currently contains documentation and no application runtime.

## What Staging Means

Staging is the remote `staging` branch plus a GitHub deployment record in the `staging` environment. Promoting to staging verifies the repository content and records the deployed commit in GitHub.

## Deploy Current Main to Staging

From a clean working tree:

```sh
scripts/deploy-staging.sh
```

The script promotes `origin/main` to `origin/staging`. Pushing `origin/staging` triggers `.github/workflows/staging-deploy.yml`.

## Deploy a Specific Ref

```sh
scripts/deploy-staging.sh --source-ref origin/main
```

Use a full commit SHA when promoting an exact revision:

```sh
scripts/deploy-staging.sh --source-ref 073fa4814b1833c9a74ec7c04a75ca05fad25e3a
```

## Dry Run

```sh
scripts/deploy-staging.sh --dry-run --allow-dirty
```

Use `--allow-dirty` when testing from a dirty working tree. Without `--allow-dirty`, the script enforces a clean working tree before it reaches the `--dry-run` behavior.

## Verify Deployment

Check the remote branch:

```sh
git ls-remote --heads origin staging
```

Check GitHub deployment records:

```sh
curl -fsS "https://api.github.com/repos/w00125533/data-benchmark/deployments?environment=staging&per_page=5"
```

Check the latest staging deployment status:

```sh
node <<'NODE'
const https = require('https');

function getJson(url) {
  return new Promise((resolve, reject) => {
    https.get(url, { headers: { 'User-Agent': 'data-benchmark-staging-check' } }, (res) => {
      let body = '';
      res.on('data', chunk => body += chunk);
      res.on('end', () => {
        if (res.statusCode < 200 || res.statusCode >= 300) {
          reject(new Error(`${url} returned HTTP ${res.statusCode}: ${body}`));
          return;
        }
        resolve(JSON.parse(body));
      });
    }).on('error', reject);
  });
}

(async () => {
  const deployments = await getJson('https://api.github.com/repos/w00125533/data-benchmark/deployments?environment=staging&per_page=1');
  const latest = deployments[0];
  if (!latest) {
    throw new Error('No staging deployment found.');
  }
  const statuses = await getJson(latest.statuses_url);
  const latestStatus = statuses[0];
  if (!latestStatus || latestStatus.state !== 'success') {
    throw new Error(`Latest staging deployment status is '${latestStatus && latestStatus.state}', expected 'success'.`);
  }
  console.log(JSON.stringify({
    state: latestStatus.state,
    created_at: latestStatus.created_at,
    description: latestStatus.description
  }, null, 2));
})();
NODE
```

The command throws unless the latest status is `success`.

## Roll Back Staging

Promote a previous commit SHA:

```sh
scripts/deploy-staging.sh --source-ref <previous-staging-commit-sha>
```

The push uses `--force-with-lease`, so it refuses unsafe remote overwrites.

## Verify Generated Web Report

After packaging the runner, generate a local smoke report:

```sh
java -jar target/data-benchmark-0.1.0-SNAPSHOT.jar run --mode local --config configs/benchmark-smoke.yml --run-id staging-web-report-smoke
```

Verify the standalone report package:

```sh
test -f reports/runs/staging-web-report-smoke/index.html
test -f reports/runs/staging-web-report-smoke/report.json
```

Open `reports/runs/staging-web-report-smoke/index.html` directly in a browser. The removed monitoring stack is not part of the report viewing path.

#!/usr/bin/env bash
set -euo pipefail

source_ref="origin/main"
remote_name="origin"
staging_branch="staging"
allow_dirty="false"
dry_run="false"

usage() {
  cat <<'EOF'
Usage: scripts/deploy-staging.sh [options]

Options:
  --source-ref <ref>       Source ref or commit to promote. Default: origin/main
  --remote-name <name>     Git remote name. Default: origin
  --staging-branch <name>  Remote staging branch. Default: staging
  --allow-dirty            Allow running with uncommitted working tree changes
  --dry-run                Print the push command without executing it
  -h, --help               Show this help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --source-ref)
      source_ref="${2:?--source-ref requires a value}"
      shift 2
      ;;
    --remote-name)
      remote_name="${2:?--remote-name requires a value}"
      shift 2
      ;;
    --staging-branch)
      staging_branch="${2:?--staging-branch requires a value}"
      shift 2
      ;;
    --allow-dirty)
      allow_dirty="true"
      shift
      ;;
    --dry-run)
      dry_run="true"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

if [[ "$allow_dirty" != "true" && -n "$(git status --porcelain)" ]]; then
  echo "Working tree has uncommitted changes. Commit/stash them or rerun with --allow-dirty." >&2
  exit 1
fi

git fetch "$remote_name" --prune

source_sha="$(git rev-parse "${source_ref}^{commit}")"
remote_staging_ref="refs/heads/${staging_branch}"
remote_staging_sha="$(git ls-remote "$remote_name" "$remote_staging_ref" | awk 'NR == 1 {print $1}')"

echo "Repository: $repo_root"
echo "Source ref: $source_ref"
echo "Source SHA: $source_sha"
if [[ -n "$remote_staging_sha" ]]; then
  echo "Current remote ${staging_branch} SHA: $remote_staging_sha"
  lease="--force-with-lease=${remote_staging_ref}:${remote_staging_sha}"
else
  echo "Remote branch ${staging_branch} does not exist and will be created."
  lease="--force-with-lease=${remote_staging_ref}:"
fi

push_ref="${source_sha}:${remote_staging_ref}"
if [[ "$dry_run" == "true" ]]; then
  echo "Dry run: git push '${lease}' '${remote_name}' '${push_ref}'"
  exit 0
fi

git push "$lease" "$remote_name" "$push_ref"
echo "Staging promotion pushed: ${remote_name}/${staging_branch} -> ${source_sha}"
echo "GitHub Actions should now run the staging deployment workflow."

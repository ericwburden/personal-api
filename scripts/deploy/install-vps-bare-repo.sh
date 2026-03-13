#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 || $# -gt 3 ]]; then
  echo "Usage: $0 <bare_repo_path> <work_tree_path> [systemd_service]"
  echo "Example: $0 /srv/git/personal-api.git /srv/personal-api personal-api.service"
  exit 1
fi

BARE_REPO="$1"
WORK_TREE="$2"
SERVICE_NAME="${3:-personal-api.service}"

mkdir -p "$(dirname "$BARE_REPO")"
mkdir -p "$WORK_TREE"

if [[ ! -d "$BARE_REPO" ]]; then
  git init --bare "$BARE_REPO"
fi

git --git-dir="$BARE_REPO" config deploy.branch production
git --git-dir="$BARE_REPO" config deploy.workTree "$WORK_TREE"
git --git-dir="$BARE_REPO" config deploy.runRenvRestore false
git --git-dir="$BARE_REPO" config deploy.restartCmd "sudo systemctl restart ${SERVICE_NAME}"

cat > "${BARE_REPO}/hooks/pre-receive" <<'HOOK'
#!/usr/bin/env bash
set -euo pipefail

is_zero_rev() {
  [[ "$1" =~ ^0+$ ]]
}

while read -r oldrev newrev refname; do
  if [[ "$refname" != "refs/heads/production" ]]; then
    continue
  fi

  range="$newrev"
  if ! is_zero_rev "$oldrev"; then
    range="${oldrev}..${newrev}"
  fi

  while read -r commit; do
    subject="$(git log --format=%s -n 1 "$commit")"
    if [[ ! "$subject" =~ ^v[0-9]+\.[0-9]+\.[0-9]+:\ .+ ]]; then
      echo "Rejected commit ${commit}"
      echo "Production commit messages must match: vMAJOR.MINOR.PATCH: <message>"
      echo "Found: ${subject}"
      exit 1
    fi
  done < <(git rev-list "$range")
done
HOOK

cat > "${BARE_REPO}/hooks/post-receive" <<'HOOK'
#!/usr/bin/env bash
set -euo pipefail

BARE_REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
BRANCH="$(git --git-dir="$BARE_REPO_DIR" config --get deploy.branch || echo production)"
WORK_TREE="$(git --git-dir="$BARE_REPO_DIR" config --get deploy.workTree)"
RUN_RENV_RESTORE="$(git --git-dir="$BARE_REPO_DIR" config --get deploy.runRenvRestore || echo false)"
RESTART_CMD="$(git --git-dir="$BARE_REPO_DIR" config --get deploy.restartCmd || true)"

if [[ -z "$WORK_TREE" ]]; then
  echo "deploy.workTree is not configured; refusing deploy."
  exit 1
fi

while read -r _oldrev _newrev refname; do
  if [[ "$refname" != "refs/heads/${BRANCH}" ]]; then
    continue
  fi

  mkdir -p "$WORK_TREE"
  GIT_WORK_TREE="$WORK_TREE" git --git-dir="$BARE_REPO_DIR" checkout -f "$BRANCH"

  if [[ "$RUN_RENV_RESTORE" == "true" ]] && command -v Rscript >/dev/null 2>&1; then
    (
      cd "$WORK_TREE"
      Rscript -e "if (requireNamespace('renv', quietly = TRUE)) renv::restore(prompt = FALSE)"
    )
  fi

  if [[ -n "${RESTART_CMD:-}" ]]; then
    bash -lc "$RESTART_CMD"
  fi

  echo "Deployed ${BRANCH} to ${WORK_TREE}"
done
HOOK

chmod +x "${BARE_REPO}/hooks/pre-receive"
chmod +x "${BARE_REPO}/hooks/post-receive"

echo "VPS bare repo configured at ${BARE_REPO}"
echo "Work tree: ${WORK_TREE}"
echo "Branch: production"
echo "Restart command: $(git --git-dir="$BARE_REPO" config --get deploy.restartCmd)"
echo
echo "If needed, allow passwordless restart for deploy user:"
echo "  sudo visudo -f /etc/sudoers.d/personal-api-deploy"
echo "  <deploy-user> ALL=(root) NOPASSWD:/bin/systemctl restart ${SERVICE_NAME}"

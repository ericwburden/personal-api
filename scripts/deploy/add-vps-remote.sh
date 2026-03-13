#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 || $# -gt 3 ]]; then
  echo "Usage: $0 <ssh_user@host> <bare_repo_path> [remote_name]"
  echo "Example: $0 deploy@example.com /srv/git/personal-api.git vps"
  exit 1
fi

SSH_TARGET="$1"
BARE_REPO_PATH="$2"
REMOTE_NAME="${3:-vps}"
REMOTE_URL="${SSH_TARGET}:${BARE_REPO_PATH}"

if git remote get-url "$REMOTE_NAME" >/dev/null 2>&1; then
  git remote set-url "$REMOTE_NAME" "$REMOTE_URL"
else
  git remote add "$REMOTE_NAME" "$REMOTE_URL"
fi

if ! git show-ref --verify --quiet refs/heads/production; then
  git branch production main
fi

git push "$REMOTE_NAME" production
git branch --set-upstream-to="${REMOTE_NAME}/production" production

echo "Configured remote '${REMOTE_NAME}' -> ${REMOTE_URL}"
echo "Pushed production branch and set upstream."

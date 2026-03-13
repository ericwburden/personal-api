#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <MAJOR.MINOR.PATCH> <message>"
  echo "Example: $0 1.2.0 \"deploy refreshed auth and notes logic\""
  exit 1
fi

VERSION="$1"
shift
MESSAGE="$*"

if [[ ! "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "Version must be semantic version format: MAJOR.MINOR.PATCH"
  exit 1
fi

BRANCH="$(git branch --show-current)"
if [[ "$BRANCH" != "production" ]]; then
  echo "This script must run on the production branch. Current branch: ${BRANCH}"
  exit 1
fi

if git diff --cached --quiet && git diff --quiet; then
  echo "No changes to commit."
  exit 1
fi

FULL_VERSION="v${VERSION}"
if git rev-parse -q --verify "refs/tags/${FULL_VERSION}" >/dev/null; then
  echo "Tag ${FULL_VERSION} already exists."
  exit 1
fi

git add -A
git commit -m "${FULL_VERSION}: ${MESSAGE}"
git tag -a "${FULL_VERSION}" -m "Release ${FULL_VERSION}"

echo "Created commit and tag ${FULL_VERSION}"
echo "Push with:"
echo "  git push origin production --follow-tags"
echo "  git push vps production --follow-tags"

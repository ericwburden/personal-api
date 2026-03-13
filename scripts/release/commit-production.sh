#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <MAJOR.MINOR.PATCH> <message>"
  echo "Example: $0 1.2.0 \"promote main to production\""
  exit 1
fi

VERSION="$1"
shift
MESSAGE="$*"

if [[ ! "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "Version must be semantic version format: MAJOR.MINOR.PATCH"
  exit 1
fi

if ! git rev-parse --verify main >/dev/null 2>&1; then
  echo "Local branch 'main' does not exist."
  exit 1
fi

if ! git rev-parse --verify production >/dev/null 2>&1; then
  echo "Local branch 'production' does not exist."
  exit 1
fi

if [[ -n "$(git status --porcelain)" ]]; then
  echo "Working tree is not clean. Commit or stash changes before promoting."
  exit 1
fi

FULL_VERSION="v${VERSION}"
if git rev-parse -q --verify "refs/tags/${FULL_VERSION}" >/dev/null; then
  echo "Tag ${FULL_VERSION} already exists."
  exit 1
fi

if git ls-remote --exit-code --tags origin "refs/tags/${FULL_VERSION}" >/dev/null 2>&1; then
  echo "Tag ${FULL_VERSION} already exists on origin."
  exit 1
fi

START_BRANCH="$(git branch --show-current)"

git fetch --prune origin

git checkout main
git pull --ff-only origin main

git checkout production
if git ls-remote --exit-code --heads origin production >/dev/null 2>&1; then
  git pull --ff-only origin production
else
  echo "origin/production does not exist yet; continuing with local production branch."
fi

COMMITS_TO_PROMOTE="$(git rev-list --count production..main)"
if [[ "${COMMITS_TO_PROMOTE}" -eq 0 ]]; then
  echo "No new commits in main to promote to production."
  if [[ "${START_BRANCH}" != "production" ]]; then
    git checkout "${START_BRANCH}"
  fi
  exit 1
fi

git merge --no-ff main -m "${FULL_VERSION}: ${MESSAGE}"
git tag -a "${FULL_VERSION}" -m "Release ${FULL_VERSION}"

if [[ "${START_BRANCH}" != "production" ]]; then
  git checkout "${START_BRANCH}"
fi

echo "Created production merge commit and tag ${FULL_VERSION}"
echo "Push with:"
echo "  git push origin production --follow-tags"
echo "  git push vps production --follow-tags"

#!/usr/bin/env bash
set -euo pipefail

git config core.hooksPath .githooks
chmod +x .githooks/pre-commit
chmod +x .githooks/commit-msg
chmod +x .githooks/pre-push

echo "Configured git hooks path to .githooks"
echo "Production commit format and push policy are now enforced locally."

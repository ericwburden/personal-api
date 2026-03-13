#!/usr/bin/env bash
set -euo pipefail

git config core.hooksPath .githooks
chmod +x .githooks/commit-msg

echo "Configured git hooks path to .githooks"
echo "Production commit message format is now enforced locally."

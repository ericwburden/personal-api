#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 || $# -gt 3 ]]; then
  echo "Usage: $0 <ssh_user@host> [app_dir] [data_dir]"
  echo "Example: $0 eric@api.ericburden.dev /home/eric/personal-api /home/eric/personal-data"
  exit 1
fi

SSH_TARGET="$1"
APP_DIR="${2:-/home/eric/personal-api}"
DATA_DIR="${3:-/home/eric/personal-data}"
SSH_OPTS=(-o BatchMode=yes -o ConnectTimeout=15 -o StrictHostKeyChecking=accept-new)
SSH_BIN="${SSH_BIN:-ssh}"

"$SSH_BIN" "${SSH_OPTS[@]}" "$SSH_TARGET" "mkdir -p '${DATA_DIR}/logs'"

cat <<EOF | "$SSH_BIN" "${SSH_OPTS[@]}" "$SSH_TARGET" "crontab -"
SHELL=/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

# Managed by scripts/deploy/overwrite-vps-crontab.sh
# Heartbeat every 15 minutes
*/15 * * * * cd ${APP_DIR} && if [ -f .env.production ]; then set -a; . ./.env.production; set +a; fi; /usr/bin/Rscript scripts/0002-heartbeat.R >> ${DATA_DIR}/logs/cron-heartbeat.log 2>&1

# Daily backup at 02:30
30 2 * * * cd ${APP_DIR} && /usr/bin/bash scripts/backup.sh >> ${DATA_DIR}/logs/cron-backup.log 2>&1
EOF

echo "Installed crontab for ${SSH_TARGET}:"
"$SSH_BIN" "${SSH_OPTS[@]}" "$SSH_TARGET" "crontab -l"

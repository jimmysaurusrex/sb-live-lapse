#!/usr/bin/env bash
set -Eeuo pipefail

if [ "${EUID}" -ne 0 ]; then
  echo "run as root"
  exit 1
fi

SERVICE_USER="sb-live-lapse"
APP_ROOT="/opt/sb-live-lapse"
REPO_DIR="${SB_REPO_DIR:-${APP_ROOT}/repo}"
PUBLISH_ROOT="${SB_PUBLISH_ROOT:-/srv/sb-live-lapse}"
REPO_URL="${SB_REPO_URL:-https://github.com/jimmysaurusrex/sb-live-lapse.git}"
SITE_ADDRESS="${SB_LAPSE_SITE:-:80}"
ENV_FILE="/etc/sb-live-lapse.env"

export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y caddy git python3 rsync sudo

if ! id -u "${SERVICE_USER}" >/dev/null 2>&1; then
  useradd --system --create-home --home-dir "${APP_ROOT}" --shell /bin/bash "${SERVICE_USER}"
fi

install -d -o "${SERVICE_USER}" -g "${SERVICE_USER}" "${APP_ROOT}" "${PUBLISH_ROOT}" "${PUBLISH_ROOT}/releases"

if [ ! -d "${REPO_DIR}/.git" ]; then
  install -d -o "${SERVICE_USER}" -g "${SERVICE_USER}" "${REPO_DIR}"
  sudo -u "${SERVICE_USER}" git clone "${REPO_URL}" "${REPO_DIR}"
else
  sudo -u "${SERVICE_USER}" git -C "${REPO_DIR}" pull --ff-only origin main
fi

if [ ! -f "${ENV_FILE}" ]; then
  cat > "${ENV_FILE}" <<EOF
SB_REPO_DIR=${REPO_DIR}
SB_PUBLISH_ROOT=${PUBLISH_ROOT}
SB_KEEP_RELEASES=3
SB_LAPSE_SITE=${SITE_ADDRESS}
SB_DEPLOYED_STATE_URL=file://${REPO_DIR}/station_state.json
SB_DEPLOYED_HISTORY_URL=file://${REPO_DIR}/station_history.json
EOF
fi

chown -R "${SERVICE_USER}:${SERVICE_USER}" "${APP_ROOT}" "${PUBLISH_ROOT}"

"${REPO_DIR}/deploy/digitalocean/deploy.sh"

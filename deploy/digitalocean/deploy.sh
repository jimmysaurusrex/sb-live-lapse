#!/usr/bin/env bash
set -Eeuo pipefail

if [ "${EUID}" -ne 0 ]; then
  echo "run as root"
  exit 1
fi

ENV_FILE="/etc/sb-live-lapse.env"
if [ -f "${ENV_FILE}" ]; then
  # shellcheck disable=SC1090
  . "${ENV_FILE}"
fi

REPO_DIR="${SB_REPO_DIR:-/opt/sb-live-lapse/repo}"
PUBLISH_ROOT="${SB_PUBLISH_ROOT:-/srv/sb-live-lapse}"
SITE_ADDRESS="${SB_LAPSE_SITE:-:80}"
SERVICE_USER="sb-live-lapse"

if [ ! -d "${REPO_DIR}/.git" ]; then
  echo "missing repo checkout at ${REPO_DIR}; run install.sh first"
  exit 1
fi

sudo -u "${SERVICE_USER}" git -C "${REPO_DIR}" pull --ff-only origin main

install -m 0644 "${REPO_DIR}/deploy/digitalocean/sb-live-lapse-refresh.service" /etc/systemd/system/sb-live-lapse-refresh.service
install -m 0644 "${REPO_DIR}/deploy/digitalocean/sb-live-lapse-refresh.timer" /etc/systemd/system/sb-live-lapse-refresh.timer

python3 - "${REPO_DIR}/deploy/digitalocean/Caddyfile" /etc/caddy/Caddyfile "${SITE_ADDRESS}" "${PUBLISH_ROOT}" <<'PY'
import sys
from pathlib import Path

template_path = Path(sys.argv[1])
output_path = Path(sys.argv[2])
site_address = sys.argv[3]
publish_root = sys.argv[4]

text = template_path.read_text()
text = text.replace("__SITE_ADDRESS__", site_address)
text = text.replace("__PUBLISH_ROOT__", publish_root)
output_path.write_text(text)
PY

systemctl daemon-reload
systemctl enable --now caddy
systemctl restart caddy
systemctl enable --now sb-live-lapse-refresh.timer
systemctl start sb-live-lapse-refresh.service

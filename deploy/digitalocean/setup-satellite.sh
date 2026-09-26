#!/usr/bin/env bash
# Keep satellite generation independent of chart acquisition and publishing.
set -Eeuo pipefail
[[ "$EUID" -eq 0 ]] || { echo 'Run as root'; exit 1; }
if [[ -f /etc/sb-live-lapse.env ]]; then
  . /etc/sb-live-lapse.env
fi
REPO_DIR="${SB_REPO_DIR:-/opt/sb-live-lapse/repo}"
PUBLISH_ROOT="${SB_PUBLISH_ROOT:-/srv/sb-live-lapse}"
RUNTIME=/opt/sb-live-lapse/satellite-venv
SERVICE_USER=sb-live-lapse

if ! "${RUNTIME}/bin/python" -m pip --version >/dev/null 2>&1; then
  if ! python3 -m venv "$RUNTIME"; then
    apt-get update -qq
    DEBIAN_FRONTEND=noninteractive NEEDRESTART_MODE=l apt-get install -y --no-install-recommends python3-venv
    python3 -m venv "$RUNTIME"
  fi
fi
"${RUNTIME}/bin/python" -m pip install --disable-pip-version-check --require-virtualenv \
  -r "${REPO_DIR}/satellite/requirements.txt"
install -d -o "$SERVICE_USER" -g "$SERVICE_USER" "${PUBLISH_ROOT}/satellite"

# Before the first main-site publish, require a real image. Subsequent upstream
# outages leave the existing image intact and do not block weather deployments.
if [[ ! -f "${PUBLISH_ROOT}/satellite/latest.json" ]]; then
  sudo -u "$SERVICE_USER" flock -w 90 "${PUBLISH_ROOT}/satellite/.refresh.lock" \
    "${RUNTIME}/bin/python" "${REPO_DIR}/satellite/build_satellite.py" \
    --output-dir "${PUBLISH_ROOT}/satellite"
fi
install -m 0644 "${REPO_DIR}/deploy/digitalocean/sb-live-lapse-satellite.service" /etc/systemd/system/
install -m 0644 "${REPO_DIR}/deploy/digitalocean/sb-live-lapse-satellite.timer" /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now sb-live-lapse-satellite.timer

# Camera views share the Pillow runtime and must exist before publishing the page.
# Keep this call in the established setup entry point so first-time promotions
# also work when deploy.sh started from the previous checkout before git pull.
bash "${REPO_DIR}/deploy/digitalocean/setup-cameras.sh"

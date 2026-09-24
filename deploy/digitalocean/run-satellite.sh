#!/usr/bin/env bash
set -Eeuo pipefail
umask 022
REPO_DIR="${SB_REPO_DIR:-/opt/sb-live-lapse/repo}"
PUBLISH_ROOT="${SB_PUBLISH_ROOT:-/srv/sb-live-lapse}"
exec 9>"${PUBLISH_ROOT}/satellite/.refresh.lock"
flock -n 9 || exit 0
/opt/sb-live-lapse/satellite-venv/bin/python \
  "${REPO_DIR}/satellite/build_satellite.py" \
  --output-dir "${PUBLISH_ROOT}/satellite"

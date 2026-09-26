#!/usr/bin/env bash
# Called after setup-satellite.sh prepares the shared production Pillow runtime.
set -Eeuo pipefail
[[ "$EUID" -eq 0 ]] || { echo 'Run as root'; exit 1; }
if [[ -f /etc/sb-live-lapse.env ]]; then
  . /etc/sb-live-lapse.env
fi
REPO_DIR="${SB_REPO_DIR:-/opt/sb-live-lapse/repo}"
PUBLISH_ROOT="${SB_PUBLISH_ROOT:-/srv/sb-live-lapse}"
RUNTIME=/opt/sb-live-lapse/satellite-venv
SERVICE_USER=sb-live-lapse
OUTPUT="${PUBLISH_ROOT}/cameras"
install -d -o "$SERVICE_USER" -g "$SERVICE_USER" "$OUTPUT"
touch "${OUTPUT}/.refresh.lock"
chown "$SERVICE_USER:$SERVICE_USER" "${OUTPUT}/.refresh.lock"
exec 8>"${OUTPUT}/.refresh.lock"
flock -w 90 8

cache_ready() {
  "${RUNTIME}/bin/python" - "${REPO_DIR}/cameras/build_cameras.py" "$OUTPUT" <<'PYCACHE'
import json, runpy, sys
from pathlib import Path
config = runpy.run_path(sys.argv[1])
output = Path(sys.argv[2])
try:
    for key, (camera_id, *_) in config['CAMERAS'].items():
        metadata = json.loads((output / f'{key}.json').read_text())
        assert metadata['camera_id'] == camera_id
        assert metadata['render_version'] == config['VERSION']
        assert (output / metadata['image']).is_file()
except (OSError, ValueError, KeyError, AssertionError):
    raise SystemExit(1)
PYCACHE
}

# A first publish or format/camera change requires the expected real images.
# Later upstream outages retain good cached images without delaying chart updates.
if ! cache_ready; then
  sudo -u "$SERVICE_USER" "${RUNTIME}/bin/python" "${REPO_DIR}/cameras/build_cameras.py" \
    --output-dir "$OUTPUT"
  cache_ready
fi
install -m 0644 "${REPO_DIR}/deploy/digitalocean/sb-live-lapse-cameras.service" /etc/systemd/system/
install -m 0644 "${REPO_DIR}/deploy/digitalocean/sb-live-lapse-cameras.timer" /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now sb-live-lapse-cameras.timer

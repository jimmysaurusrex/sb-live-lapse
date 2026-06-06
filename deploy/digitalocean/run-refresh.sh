#!/usr/bin/env bash
set -Eeuo pipefail

umask 022

REPO_DIR="${SB_REPO_DIR:-/opt/sb-live-lapse/repo}"
PUBLISH_ROOT="${SB_PUBLISH_ROOT:-/srv/sb-live-lapse}"
KEEP_RELEASES="${SB_KEEP_RELEASES:-3}"
LOCK_FILE="${PUBLISH_ROOT}/.refresh.lock"
RELEASES_DIR="${PUBLISH_ROOT}/releases"
CURRENT_LINK="${PUBLISH_ROOT}/current"
NEXT_LINK="${PUBLISH_ROOT}/.next-current"

mkdir -p "${RELEASES_DIR}"

exec 9>"${LOCK_FILE}"
if ! flock -n 9; then
  echo "refresh already running"
  exit 0
fi

cd "${REPO_DIR}"
python3 replot_recent60_sba.py

stamp="$(date -u +%Y%m%dT%H%M%SZ)"
stage_dir="${RELEASES_DIR}/${stamp}"
mkdir -p "${stage_dir}"

files=(
  index.html
  app.js
  styles.css
  sba_wwtemp_chart.svg
  sba_wwtemp_chart_metric.svg
  sba_wwtemp_chart_imperial.svg
  station_state.json
  station_history.json
)

for path in "${files[@]}"; do
  install -m 0644 "${path}" "${stage_dir}/${path}"
done

if [ -d snapshots ]; then
  cp -R snapshots "${stage_dir}/snapshots"
fi

python3 - "${stage_dir}/healthz.json" <<'PY'
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

state = {}
state_path = Path("station_state.json")
if state_path.exists():
    try:
        state = json.loads(state_path.read_text())
    except Exception:
        state = {}

payload = {
    "healthy": True,
    "generated_at": state.get("generated_at"),
    "published_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "source": "digitalocean-droplet",
}
Path(sys.argv[1]).write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
PY

ln -sfn "${stage_dir}" "${NEXT_LINK}"
mv -Tf "${NEXT_LINK}" "${CURRENT_LINK}"

python3 - "${RELEASES_DIR}" "${KEEP_RELEASES}" <<'PY'
import shutil
import sys
from pathlib import Path

root = Path(sys.argv[1])
keep = max(1, int(sys.argv[2]))
dirs = sorted([path for path in root.iterdir() if path.is_dir()])
for path in dirs[:-keep]:
    shutil.rmtree(path, ignore_errors=True)
PY

echo "published_release=${stage_dir}"

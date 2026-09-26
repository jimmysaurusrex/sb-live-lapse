#!/usr/bin/env bash
# Separate code, web root, timer and data. No primary checkout or assets are edited.
set -Eeuo pipefail
[[ "$EUID" -eq 0 ]] || { echo 'Run as root'; exit 1; }
revision="${1:?Missing revision}"
[[ "$revision" =~ ^[a-f0-9]{40}$ ]] || { echo 'Invalid revision'; exit 1; }
source_dir="$(cd "$(dirname "$0")/.." && pwd)"
beta_root=/srv/sb-live-lapse-beta
code_root=/opt/sb-live-lapse-beta
primary_root=/srv/sb-live-lapse/current
route_file=/etc/caddy/sb-live-lapse-beta.caddy
config_file=/etc/caddy/Caddyfile
marker='    import /etc/caddy/sb-live-lapse-beta.caddy'

test -f "${primary_root}/index.html"
test -f "${primary_root}/station_history.json"
systemctl is-active --quiet caddy
systemctl is-active --quiet sb-live-lapse-refresh.timer
primary_hash="$(sha256sum "${primary_root}/index.html" "${primary_root}/app.js" "${primary_root}/styles.css")"

install -d -o sb-live-lapse -g sb-live-lapse "${beta_root}" "${beta_root}/releases"
install -d "${code_root}"
# Rendering dependencies belong only to beta, never to the primary application.
if ! "${code_root}/satellite-venv/bin/python" -m pip --version >/dev/null 2>&1; then
    if ! python3 -m venv "${code_root}/satellite-venv"; then
        apt-get update -qq
        DEBIAN_FRONTEND=noninteractive NEEDRESTART_MODE=l apt-get install -y --no-install-recommends python3-venv
        python3 -m venv "${code_root}/satellite-venv"
    fi
fi
"${code_root}/satellite-venv/bin/python" -m pip install --disable-pip-version-check \
    --require-virtualenv -r "${source_dir}/requirements-satellite.txt"
install -d -o sb-live-lapse -g sb-live-lapse "${beta_root}/satellite" "${beta_root}/cameras"
stage="${beta_root}/releases/${revision}"
install -d -o sb-live-lapse -g sb-live-lapse "$stage"
for asset in index.html styles.css app.js; do
    install -m 0644 "${source_dir}/${asset}" "${stage}/${asset}"
done
ln -sfn ../../satellite "${stage}/satellite"
ln -sfn ../../cameras "${stage}/cameras"
for artifact in station_state.json station_history.json sba_wwtemp_chart.svg sba_wwtemp_chart_metric.svg sba_wwtemp_chart_imperial.svg snapshots; do
    ln -sfn "../../chart-data/${artifact}" "${stage}/${artifact}"
done

# Generate beta data before exposing the route. A failure cannot affect primary publishing.
sudo -u sb-live-lapse python3 "${source_dir}/build_charts.py" \
    --primary-dir "$primary_root" --output-dir "${beta_root}/chart-data"
python3 - "${beta_root}/chart-data/station_state.json" <<'PY'
import json, sys
data = json.load(open(sys.argv[1]))
assert data.get('stations'), 'Beta profile missing'
PY
# Require a real crop before publishing the first satellite release. Use the
# same lock as its independent timer when subsequent beta releases are installed.
sudo -u sb-live-lapse flock -w 90 "${beta_root}/.satellite.lock" \
    "${code_root}/satellite-venv/bin/python" "${source_dir}/build_satellite.py" \
    --output-dir "${beta_root}/satellite"

# Camera rendering is isolated from both chart generation and satellite refresh.
sudo -u sb-live-lapse flock -w 90 "${beta_root}/.cameras.lock" \
    "${code_root}/satellite-venv/bin/python" "${source_dir}/build_cameras.py" \
    --output-dir "${beta_root}/cameras"
for camera in gibraltar tvhill; do test -s "${beta_root}/cameras/${camera}.json"; done

# Validate a candidate config first. Apart from one /beta-only import, the
# existing site's configuration remains byte-for-byte identical.
backup_dir="${code_root}/config-backups/${revision}-$(date -u +%Y%m%dT%H%M%SZ)"
install -d "$backup_dir"
cp "$config_file" "${backup_dir}/Caddyfile"
if [[ -f "$route_file" ]]; then cp "$route_file" "${backup_dir}/beta.caddy"; fi
install -m 0644 "${source_dir}/deploy/beta.caddy" "$route_file"
candidate="${backup_dir}/Caddyfile.candidate"
python3 - "$config_file" "$candidate" <<'PY'
from pathlib import Path
import sys
original = Path(sys.argv[1]).read_text()
marker = '    import /etc/caddy/sb-live-lapse-beta.caddy\n'
if any(line.strip() in ('import /etc/caddy/sb-live-lapse-beta.caddy',
                        'import /etc/caddy/sb-live-lapse-beta*.caddy')
       for line in original.splitlines()):
    candidate = original
else:
    anchor = '\tfile_server\n'
    if original.count(anchor) != 1 or 'root * /srv/sb-live-lapse/current' not in original:
        raise SystemExit('Unexpected Caddy layout; leaving active config untouched')
    candidate = original.replace(anchor, marker + anchor)
    assert candidate.replace(marker, '') == original
Path(sys.argv[2]).write_text(candidate)
PY
if ! caddy validate --config "$candidate" --adapter caddyfile; then
    if [[ -f "${backup_dir}/beta.caddy" ]]; then cp "${backup_dir}/beta.caddy" "$route_file"; fi
    exit 1
fi

ln -sfn "$source_dir" "${code_root}/.next"
mv -Tf "${code_root}/.next" "${code_root}/current"
ln -sfn "$stage" "${beta_root}/.next"
mv -Tf "${beta_root}/.next" "${beta_root}/current"
install -m 0644 "$candidate" "$config_file"
if ! systemctl reload caddy; then
    cp "${backup_dir}/Caddyfile" "$config_file"
    if [[ -f "${backup_dir}/beta.caddy" ]]; then cp "${backup_dir}/beta.caddy" "$route_file"; fi
    systemctl reload caddy
    exit 1
fi
install -m 0644 "${source_dir}/deploy/sb-live-lapse-beta.service" /etc/systemd/system/
install -m 0644 "${source_dir}/deploy/sb-live-lapse-beta.timer" /etc/systemd/system/
install -m 0644 "${source_dir}/deploy/sb-live-lapse-beta-satellite.service" /etc/systemd/system/
install -m 0644 "${source_dir}/deploy/sb-live-lapse-beta-satellite.timer" /etc/systemd/system/
install -m 0644 "${source_dir}/deploy/sb-live-lapse-beta-cameras.service" /etc/systemd/system/
install -m 0644 "${source_dir}/deploy/sb-live-lapse-beta-cameras.timer" /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now sb-live-lapse-beta.timer
systemctl enable --now sb-live-lapse-beta-satellite.timer
systemctl enable --now sb-live-lapse-beta-cameras.timer
systemctl start sb-live-lapse-beta.service
test "$primary_hash" = "$(sha256sum "${primary_root}/index.html" "${primary_root}/app.js" "${primary_root}/styles.css")"
systemctl is-active --quiet sb-live-lapse-refresh.timer
systemctl is-active --quiet caddy
echo "Published beta revision ${revision}; primary page assets unchanged"

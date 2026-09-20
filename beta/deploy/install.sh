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
stage="${beta_root}/releases/${revision}"
install -d -o sb-live-lapse -g sb-live-lapse "$stage"
for asset in index.html styles.css app.mjs model.mjs; do
    install -m 0644 "${source_dir}/${asset}" "${stage}/${asset}"
done
ln -sfn ../../data.json "${stage}/data.json"

# Generate beta data before exposing the route. A failure cannot affect primary publishing.
sudo -u sb-live-lapse python3 "${source_dir}/build_data.py" \
    --primary-dir "$primary_root" --output "${beta_root}/data.json"
python3 - "${beta_root}/data.json" <<'PY'
import json, sys
data = json.load(open(sys.argv[1]))
assert data.get('version') == 1 and data.get('profile', {}).get('stations'), 'Beta profile missing'
PY

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
if marker in original:
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
systemctl daemon-reload
systemctl enable --now sb-live-lapse-beta.timer
systemctl start sb-live-lapse-beta.service
test "$primary_hash" = "$(sha256sum "${primary_root}/index.html" "${primary_root}/app.js" "${primary_root}/styles.css")"
systemctl is-active --quiet sb-live-lapse-refresh.timer
systemctl is-active --quiet caddy
echo "Published beta revision ${revision}; primary page assets unchanged"

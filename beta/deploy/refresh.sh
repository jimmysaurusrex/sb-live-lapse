#!/usr/bin/env bash
set -Eeuo pipefail
umask 022
exec 9>/srv/sb-live-lapse-beta/.refresh.lock
flock -n 9 || exit 0
python3 /opt/sb-live-lapse-beta/current/build_charts.py \
  --primary-dir /srv/sb-live-lapse/current \
  --output-dir /srv/sb-live-lapse-beta/chart-data

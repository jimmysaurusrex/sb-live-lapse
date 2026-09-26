#!/usr/bin/env bash
set -Eeuo pipefail
umask 022
exec 9>/srv/sb-live-lapse-beta/.cameras.lock
flock -n 9 || exit 0
/opt/sb-live-lapse-beta/satellite-venv/bin/python \
  /opt/sb-live-lapse-beta/current/build_cameras.py \
  --output-dir /srv/sb-live-lapse-beta/cameras

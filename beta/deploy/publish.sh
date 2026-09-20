#!/usr/bin/env bash
# Publish only the committed beta using an existing administrator connection.
# Does not grant the primary CI account new privileges.
set -Eeuo pipefail
target="${1:?Usage: publish.sh user@host /path/to/existing/ssh/key}"
identity="${2:?Missing SSH identity path}"
[[ "$target" =~ ^[a-z_][a-z0-9_-]*@[A-Za-z0-9.-]+$ ]] || { echo 'Invalid SSH target'; exit 1; }
repo="$(cd "$(dirname "$0")/../.." && pwd)"
revision="$(git -C "$repo" rev-parse HEAD)"
[[ "$revision" =~ ^[a-f0-9]{40}$ ]]
bundle_dir="$(mktemp -d)"
trap 'rm -rf "$bundle_dir"' EXIT
archive="${bundle_dir}/cloud-beta.tgz"
git -C "$repo" archive --format=tar.gz --output="$archive" "$revision" beta
ssh_options=(-i "$identity" -o BatchMode=yes -o IdentitiesOnly=yes -o StrictHostKeyChecking=yes -o ConnectTimeout=10)
scp "${ssh_options[@]}" "$archive" "${target}:/tmp/cloud-beta-${revision}.tgz"
ssh "${ssh_options[@]}" "$target" "bash -s -- ${revision}" <<'REMOTE'
set -Eeuo pipefail
revision="$1"
[[ "$revision" =~ ^[a-f0-9]{40}$ ]]
[[ "$EUID" -eq 0 ]] || { echo 'An existing root administrator connection is required'; exit 1; }
release="/opt/sb-live-lapse-beta/releases/${revision}"
mkdir -p "$release"
tar -xzf "/tmp/cloud-beta-${revision}.tgz" -C "$release"
bash "${release}/beta/deploy/install.sh" "$revision"
rm "/tmp/cloud-beta-${revision}.tgz"
REMOTE

# Compact dew-point beta

Preview: https://sb-live-lapse.com/beta/

This beta keeps the primary site's compact layout, SVG chart, wind barbs, lapse
rates, station rows, metric/imperial controls, and snapshot navigation. The change
is a temperature/dew-point pair at each station, both on the graph and in the
station list: `86.3F/53.9F` or `30.2C/12.2C`. Missing dew point is `86.3F/—`.

`build_charts.py` reads an existing primary release and adds dew points to its SVG
text labels. The primary chart's colors, dimensions, axes, geometry and weather
calculations are preserved. Labels near the right edge can flip left using the
original chart's sizing rule. Each historical chart uses its own snapshot's dew
points. Source observation times and missing/stale behavior remain those of the
primary chart. No additional cloud feeds or imagery are fetched.

Primary HTML, CSS, JavaScript, SVGs, data, checkout and refresh job are read-only
inputs. Beta keeps its own unit preference (`sb_beta_units`). Its HTML and CSS
match the primary, with a beta page title/canonical URL and a link to the existing
FAQ. The earlier cloud-context experiment remains available in git history.

## Local preview and checks

```sh
python3 -m unittest discover -s tests -v
python3 -m unittest discover -s beta/tests -v
python3 beta/build_charts.py --primary-dir /path/to/primary-release --output-dir beta/preview
cp beta/index.html beta/app.js beta/styles.css beta/preview/
python3 -m http.server 8765 --bind 127.0.0.1
```

Open http://127.0.0.1:8765/beta/preview/. Generated files are ignored by git.

## Publishing and operations

Push `codex/cloud-beta` to run validation in `.github/workflows/deploy-beta.yml`.
Publish the tested, committed beta with the existing administrator connection:

```sh
bash beta/deploy/publish.sh root@YOUR_DROPLET /path/to/existing/administrator/key
```

The publisher uploads only committed beta code. The installer builds the SVGs
before exposing them, validates Caddy, and gracefully reloads it. It checks primary
asset hashes and that the primary refresh timer is still active. It does not
invoke the primary deployment or grant its CI account new privileges.

- Code: `/opt/sb-live-lapse-beta/releases/<sha>/beta`, atomic `current` symlink.
- Web assets: `/srv/sb-live-lapse-beta/releases/<sha>`, atomic `current` symlink.
- Generated charts/history: `/srv/sb-live-lapse-beta/chart-data`, linked from the
  beta web release. Individual files are written atomically; unchanged files are
  retained. Only beta snapshot files outside the primary history are pruned.
- Timer/service: `sb-live-lapse-beta.timer` / `sb-live-lapse-beta.service`, every
  five minutes. The service can only write to `/srv/sb-live-lapse-beta`.
- Caddy: `/etc/caddy/sb-live-lapse-beta.caddy`, imported by the existing Caddyfile.
  Configuration backups are in `/opt/sb-live-lapse-beta/config-backups`.

A full **primary code deployment** regenerates Caddy's configuration and may
remove the beta import; redeploy beta afterward. Routine primary weather refreshes
do not change routing or beta files.

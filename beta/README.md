# Compact chart beta

Preview: https://sb-live-lapse.com/beta/

This beta keeps the primary site's compact layout, SVG chart, wind barbs, lapse
rates, station rows, metric/imperial controls, and snapshot navigation. The change
is a temperature/dew-point-spread pair at each station, both on the graph and in
the station list: `63.5F/+1.3F` means a dew point of `62.2F`. The spread uses a
leading plus sign, one decimal place, and a leading zero below one degree
(`+0.5F`). A spread that rounds to zero in the selected units is shown as
`/saturated`. Missing dew point is `63.5F/—`. Celsius works the same way; Fahrenheit
spreads are converted as differences, without adding 32.

`build_charts.py` reads an existing primary release and adds dew-point spreads to
its SVG text labels. The primary chart's colors, dimensions, axes, geometry and weather
calculations are preserved. Labels near the right edge can flip left using the
original chart's sizing rule. Each historical chart uses its own snapshot's dew
points. Source observation times and missing/stale behavior remain those of the
primary chart. No additional station or cloud-observation feeds are fetched.

Primary HTML, CSS, JavaScript, SVGs, data, checkout and refresh job are read-only
inputs. Beta keeps its own unit preference (`sb_beta_units`), the primary chart
styles, a beta page title/canonical URL, and a link to the existing FAQ. The earlier
cloud-context experiment remains available in git history.

The beta time widget keeps the arrows together around a Pacific-day dropdown and
an editable 24-hour time: `← Weds @ 14:40 →`. The dropdown lists only days with
available history. Enter `HH:MM` or `HHMM` and press Enter or leave the field to
jump to the nearest available snapshot on that day; the field then displays the
snapshot's actual time. Arrow navigation and unit changes keep both fields in
sync. Escape cancels an edit. Missing history leaves the fields disabled while
the latest chart remains usable.

The bottom of the page shows a north-up Santa Barbara satellite crop, from Painted
Cave to Rincon with a small surrounding margin (119.85–119.42 W, 34.32–34.55 N).
The coastline and Painted Cave, Santa Barbara, Carpinteria and Rincon are marked.
CIRA/NOAA GOES-West full-disk tiles provide native Band 2 visible imagery by day
(nominal 0.5 km at nadir) and Nighttime Microphysics at night (2 km). The local
footprints are larger. CIRA's grid navigation is used to reproject the small crop
with nearest-neighbor sampling; enlarging pixels does not create extra detail.
The visible image uses fixed gamma enhancement. Solar elevation at the image
scan time selects the product; twilight is labeled. Neither view measures cloud
base or sees low cloud hidden under an opaque upper layer.

Scans are ten minutes apart, with additional publication latency. The page shows
the actual scan start time in Pacific time, its age (updated without network
traffic), and a delayed label after 30 minutes. Imagery is independent of the
selected historical weather chart. A compact still is the default (~45 KB in the
first real preview); a last-hour GIF loop (~315 KB in that preview, scene-dependent)
is downloaded only after tapping Play, whose label includes its size. Stop,
chart navigation or hiding the tab returns to the still. The loop download is
retained for replay. There are no background image downloads or automatic refreshes.

Both the small metadata request and the image request wait for window load, both
chart-data fetches to settle, the selected chart image to load successfully, and
the satellite section to enter the actual viewport. Images have no initial src,
preload or preconnect. Fetches have low priority and can be aborted when a chart
changes or the page hides. Save-Data/2G/3G connections reported by the browser and
browsers without IntersectionObserver require a tap. Taps cannot bypass the chart
readiness gate. Failed requests require explicit retry. Browser requests stay on
our own server; upstream tile downloads and cropping happen in a separate,
resource-limited beta satellite service, never in a visitor's browser.

`build_satellite.py` publishes dated JPEGs and an optional GIF before atomically
replacing `latest.json`. It reuses existing frames, tolerates a not-yet-published
newest tile, never replaces a good image with an older one, and retains the last
good output on upstream failure. The frontend's age label then exposes the delay.
Only generated satellite artifacts older than 24 hours are pruned. Attribution
and projection details are recorded in `data/README.md`.

## Local preview and checks

Satellite rendering requires Python 3.10 or newer.

```sh
python3 -m pip install -r beta/requirements-satellite.txt
python3 -m unittest discover -s tests -v
python3 -m unittest discover -s beta/tests -v
node --test beta/tests/test_*.cjs
python3 beta/build_charts.py --primary-dir /path/to/primary-release --output-dir beta/preview
cp beta/index.html beta/app.js beta/styles.css beta/preview/
python3 beta/build_satellite.py --output-dir beta/preview/satellite
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
- Satellite service/timer: `sb-live-lapse-beta-satellite.service` / `.timer`,
  checks every five minutes independently of the chart timer; 90-second limit,
  25% CPU quota, low CPU/I/O priority, 192 MB memory limit. Its Pillow dependency
  lives in `/opt/sb-live-lapse-beta/satellite-venv`; outputs in
  `/srv/sb-live-lapse-beta/satellite`. No main-site Python dependencies change.
- Caddy: `/etc/caddy/sb-live-lapse-beta.caddy`, imported by the existing Caddyfile.
  Configuration backups are in `/opt/sb-live-lapse-beta/config-backups`.

The primary deployment preserves the optional beta route. Routine primary
weather refreshes do not change routing or beta files.

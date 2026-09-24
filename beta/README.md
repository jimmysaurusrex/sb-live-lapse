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

The bottom of the page includes the original CIRA/NOAA GOES-West GeoColor image
for the Los Angeles/Oxnard region. It is the latest NOAA image, independent of the
selected historical chart; its observation timestamp is printed on the image.
Only one 600×600 JPEG is downloaded (roughly 400 KB, varying by scene), with no
animation, metadata fetch, preload, or preconnect. The image has no initial `src`.

Its request waits for window load, both chart-data fetches to settle, the selected
chart image to load successfully, and the satellite section to enter the actual
viewport. It uses low fetch priority and asynchronous image decoding. Save-Data,
2G, or 3G connections reported by the browser require a tap; browsers without
IntersectionObserver also show a load button. The button cannot bypass the chart
readiness gate. Changing charts or hiding the page aborts a pending satellite
download; it can resume after the chart is ready. Failed downloads require an
explicit retry. Successfully loaded images are retained across chart navigation
and are not refreshed automatically.

## Local preview and checks

```sh
python3 -m unittest discover -s tests -v
python3 -m unittest discover -s beta/tests -v
node --test beta/tests/test_*.cjs
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

The primary deployment preserves the optional beta route. Routine primary
weather refreshes do not change routing or beta files.

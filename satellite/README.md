# Santa Barbara satellite view

The bottom of the page shows a north-up Santa Barbara satellite crop, from Painted
Cave to Rincon with a small surrounding margin (119.85–119.42 W, 34.32–34.55 N).
The coastline and Painted Cave, Santa Barbara, Carpinteria and Rincon are marked.
CIRA/NOAA GOES-West full-disk tiles provide native Band 2 visible imagery by day
(nominal 0.5 km at nadir) and Nighttime Microphysics at night (2 km). The local
footprints are larger. CIRA's grid navigation is used to reproject the small crop
with nearest-neighbor sampling; enlarging pixels does not create extra detail.
The visible image uses fixed gamma enhancement. Solar elevation at the image
scan time selects the product; twilight is included in the accessible description. Neither view measures cloud
base or sees low cloud hidden under an opaque upper layer.

Scans are ten minutes apart, with additional publication latency. The visible
section contains only its heading, a date/time line above each image frame, the
map, and the Play/Stop button. The actual scan start time is Pacific time and
changes with each animation frame. Image age, delay and twilight details remain
in a hidden accessible description, updated without network traffic. Source
attribution is in the image tooltip. Imagery is independent of the selected
historical weather chart. A compact still is the default (~45 KB in the
first real preview); a last-hour GIF loop (~315 KB in that preview, scene-dependent)
is downloaded only after tapping Play, whose tooltip includes its size. Stop,
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
resource-limited satellite service, never in a visitor's browser.

`build_satellite.py` publishes dated JPEGs and an optional GIF before atomically
replacing `latest.json`. It reuses existing frames, tolerates a not-yet-published
newest tile, never replaces a good image with an older one, and retains the last
good output on upstream failure. The displayed scan time remains unchanged; its accessible description also flags the delay.
Only generated satellite artifacts older than 24 hours are pruned. Attribution
and projection details are recorded in `data/README.md`.

## Checks and deployment

Python 3.10+ and the pinned Pillow version in `requirements.txt` are required.
The renderer and frontend were promoted from the approved compact beta.

```sh
python3 -m pip install -r satellite/requirements.txt
python3 -m unittest discover -s tests -v
node --test tests/test_*.cjs
python3 satellite/build_satellite.py --output-dir /tmp/sb-satellite-preview
```

Production uses its own `/opt/sb-live-lapse/satellite-venv` and
`/srv/sb-live-lapse/satellite` output directory. `setup-satellite.sh` installs the
independent `sb-live-lapse-satellite.service` and `.timer`; they never read or
write beta assets. The timer checks every five minutes, under a 90-second time
limit, 25% CPU quota, low CPU/I/O priority, and 192 MB memory limit. Chart refreshes
only link the existing satellite directory into each atomic site release.

Deployment requires a real image before the initial satellite publish. Subsequent
upstream failures retain the last good image and do not block chart publishing.
The GitHub Pages build also attempts a crop, but a satellite outage does not fail
its weather-chart publish. The canonical site remains the DigitalOcean deployment.

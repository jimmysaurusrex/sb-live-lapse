# Santa Barbara camera views

The camera grid has two rows: GOES alongside the latest tight Gibraltar 2 image
(1986), then a TV Hill 2 panorama cropped from 330° through north to 090° (2748).
The TV Hill crop spans 120°, centered on 030°, with bearing labels. Ortega has been
removed from the page and refresh job. Gibraltar 2 uses its actual single tight
shot, resized without changing its field of view; the actual bearing is printed
beside its Pacific timestamp because the camera can move. Its current heading is
approximately south. No physical camera controls are used.

`build_cameras.py` fetches public ALERTCalifornia/UC San Diego metadata from
`https://api.cdn.prod.alertwest.com/api/getCameraDataByLoc` for Gibraltar 2 and
`https://api.cdn.prod.alertwest.com/api/panorama/list/byCamId?camId=2748&timestamp=`
for TV Hill. JPEGs come from `https://img.cdn.prod.alertwest.com/data/img/`.
All upstream traffic stays on the server, which requests gzip-compressed metadata.
The browser downloads only a 600px tight view and 1200px panorama crop with Pacific
timestamps. The panorama API supplies midpoint azimuth and angular width; the crop
wraps at north. The cache updates every two minutes in an independent production service.
Each view uses the same chart-first, viewport-only, abortable loader as GOES,
with no automatic browser refresh. Feed failures preserve the last good image;
a camera older than 15 minutes displays a compact delay label. Titles link to
ALERTCalifornia and image tooltips retain attribution. These views always show
latest camera imagery, independently of the selected historical chart.

## Deployment

`build_cameras.py` is the approved beta renderer. It uses the production Pillow
runtime at `/opt/sb-live-lapse/satellite-venv`, installed by
`deploy/digitalocean/setup-satellite.sh`. That established setup entry point also
calls `setup-cameras.sh` before publishing the main page. Setup requires images
with the expected camera IDs and renderer version on the first deployment; valid
cached images are retained during later upstream outages.

`sb-live-lapse-cameras.service` and `.timer` update `/srv/sb-live-lapse/cameras`
every two minutes, independently of charts and satellite imagery. The service
has a 90-second limit, a 25% CPU quota, low CPU/I/O priority, a 256 MB memory limit,
and write access only to the camera output directory. A file lock prevents
concurrent updates. The chart publisher links the existing camera directory into
each atomic release and never waits for a camera network fetch. Metadata is
served with `Cache-Control: no-store`.

The main site's cache and timer are independent of `/beta`. GitHub Pages also
attempts camera generation without making its weather publish depend on camera
availability.

```sh
python3 -m pip install -r satellite/requirements.txt
python3 -m unittest discover -s tests -v
node --test tests/test_*.cjs
python3 cameras/build_cameras.py --output-dir /tmp/sb-camera-preview
```

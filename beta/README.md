# Cloud beta

Public preview: https://sb-live-lapse.com/beta/

The beta runs independently of the primary site. Only `beta/` and its dedicated
deployment workflow are added on `codex/cloud-beta`; the primary checkout, assets,
data writer, deployment workflow and refresh timer are unchanged.

## Data and interpretation

- The station/RASS profile is a read-only snapshot of primary `station_history.json`.
  Temperature/dew-point spread is a moisture hint, not cloud detection. Fields may
  have different observation times in the upstream station data. Cached and
  temperature observations older than 60 minutes are excluded from saturation
  flags and the VOR LCL estimate. Invalid dew points above temperature are omitted.
- KSBA METAR cloud bases are feet AGL, converted to meters MSL using the report's
  airport elevation. Coverage and base are plotted; tops are not inferred. Missing,
  clear, obscured and stale reports are distinct. Reports expire at 90 minutes.
- VOR LCL uses station elevation + 125 m per °C of temperature/dew-point spread.
  It estimates lifted-parcel condensation, not existing cloud.
- NOAA/CIRA GOES-West GeoColor provides regional cloud context. Dated images expire
  at 60 minutes; their time comes from the filename, not fetch time.
- ALERTCalifornia camera 1986 is Gibraltar 2, at 34.465286, -119.678314, approximately
  0.67 km south of AntFarm (34.47121, -119.67688). The public camera's linked location
  and image timestamp come from its public metadata. Images expire at 15 minutes;
  offline cameras are hidden. Camera heading changes as the camera rotates.
  Credit: ALERTCalifornia | UC San Diego. Public imagery policy:
  https://alertcalifornia.org/images-and-video/

Source failures retain original observation times. Each source fails independently.
The browser enforces freshness even if the whole beta refresh stops. The beta uses
its own local-storage setting and restricts image URLs to the selected public feeds.

## Development

```sh
python3 -m unittest discover -s beta/tests -v
node --test beta/tests/model.test.mjs
python3 beta/build_data.py --output beta/data.json
python3 -m http.server 8765 --bind 127.0.0.1
```

Open http://127.0.0.1:8765/beta/. Generated data is ignored by git.

## Operations

Push `codex/cloud-beta` to run `.github/workflows/deploy-beta.yml` using the existing
DigitalOcean SSH secrets. It runs primary and beta tests, uploads only the beta
directory, generates data before publishing, validates Caddy, and gracefully
reloads it. It never invokes the primary deployment or primary refresh.

- Code: `/opt/sb-live-lapse-beta/releases/<sha>/beta`, atomic `current` symlink.
- Web assets: `/srv/sb-live-lapse-beta/releases/<sha>`, atomic `current` symlink.
- Data: `/srv/sb-live-lapse-beta/data.json`, atomically replaced every five minutes.
- Timer/service: `sb-live-lapse-beta.timer` / `sb-live-lapse-beta.service`.
  The service can only write to `/srv/sb-live-lapse-beta`.
- Caddy: `/etc/caddy/sb-live-lapse-beta.caddy`, imported by the existing Caddyfile.
  The installer verifies primary asset hashes and that its timer stays active.
  Backups of Caddy configuration are in `/opt/sb-live-lapse-beta/config-backups`.

The sole shared-server change is the `/beta` route and a graceful Caddy reload.
A future full **primary deployment** regenerates Caddy's main configuration and
may remove that import; redeploy this beta afterward. Routine primary refreshes
do not change routing or beta files. If the beta is promoted later, preserve its
route explicitly in the primary deployment template at that time.

To disable the preview, stop/disable `sb-live-lapse-beta.timer`, remove its single
import from `/etc/caddy/Caddyfile`, validate Caddy and reload it. Do not restore an
old full configuration if other server configuration has since changed.

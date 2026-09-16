# SB Live Lapse

This repo currently publishes the chart through GitHub Pages via [`.github/workflows/refresh.yml`](.github/workflows/refresh.yml). That path stays in place until the DigitalOcean droplet is live and you are happy with the cutover.

The DigitalOcean deployment assets live under [`deploy/digitalocean/`](deploy/digitalocean). They add a separate, timer-driven hosting path:

- the existing GitHub Pages workflow keeps publishing as best it can
- a droplet can refresh locally every 5 minutes with `systemd`
- a separate GitHub Actions workflow can manually deploy to the droplet before auto-deploy is enabled

The new droplet workflow is intentionally conservative by default. Manual droplet deploys work once you add the droplet secrets, and automatic deploys on push only start after you set the repository variable `DO_DEPLOY_ENABLED=true`.

## Station feeds

La Cumbre retains internal call sign `KC6OYN` (letter O) and display ID `KC60YN`.
Its normal source is NOAA MADIS over certificate-verified HTTPS, queried with
the assigned MADIS ID **`AV377`**. The previous `stanam=KC6OYN` query returned an
empty mesonet, unnecessarily forcing every refresh through findU.

The [MesoWest station listing](https://mesowest.utah.edu/cgi-bin/droman/nearby_stns.cgi?stn=467SE)
identifies `AV377` as “KC6OYN Santa Barbara.” On September 16, 2026, a verified
HTTPS request to the [MADIS XML endpoint](https://madis-data.ncep.noaa.gov/madisPublic/cgi-bin/madisXmlPublicDir?time=0&minbck=-59&minfwd=0&recwin=3&timefilter=0&dfltrsel=3&stasel=1&stanam=AV377&pvdrsel=0&varsel=2&qctype=0&qcsel=1&xml=1&csvmiss=0)
returned a fresh APRSWXNET observation at 21:26 UTC (17.22 C, 6.26 m/s wind),
while findU HTTPS failed certificate verification. This source requires no API
key. It authenticates our connection to NOAA; it does not cryptographically
authenticate the station's original APRS packets. NOAA describes the upstream
CWOP ingestion path in its [CWOP FAQ](https://madis.ncep.noaa.gov/faq_cwop.shtml).

If MADIS has no valid recent temperature, findU is tried over HTTPS only.
There is **no HTTP fallback or TLS-verification bypass**, including redirects.
If both feeds fail, the existing last-good cache keeps readings available for up
to 90 minutes; only observations aged 0–60 minutes are plotted as recent. The
other stations and chart refresh continue when La Cumbre is unavailable.

Both XML feeds require matching station IDs, finite values within broad physical
bounds, and observations aged 0–60 minutes; future timestamps are rejected. The newest
valid observation wins, so an invalid newest report cannot hide a usable older
one. Invalid optional humidity/wind fields are omitted. Responses are capped at
256 KiB, DTDs/entities and redirects are rejected, and feed failures are logged.

`provider` in CSV/state/history includes the source station and HTTPS transport.
`temp_source` in state/history records the temperature's service, queried station
ID, URL, transport, and (for MADIS) reported elevation. Cached temperatures retain
their original provenance and observation time, with a `(last-good)` provider
suffix. Legacy cached rows without provenance remain unspecified.

**Elevation discrepancy:** MADIS reports 820.96 m for AV377, while this chart has
an existing configured La Cumbre elevation of 1201 m (3940 ft). This feed repair
preserves that chart setting; the MADIS value is retained in
`temp_source.reported_elev_m` so the discrepancy remains visible for a separate
station-metadata review.

Run the feed regression checks with `python3 -m unittest discover -s tests -v`.

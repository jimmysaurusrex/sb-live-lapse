# SB Live Lapse

This repo currently publishes the chart through GitHub Pages via [`.github/workflows/refresh.yml`](.github/workflows/refresh.yml). That path stays in place until the DigitalOcean droplet is live and you are happy with the cutover.

The DigitalOcean deployment assets live under [`deploy/digitalocean/`](deploy/digitalocean). They add a separate, timer-driven hosting path:

- the existing GitHub Pages workflow keeps publishing as best it can
- a droplet can refresh locally every 5 minutes with `systemd`
- a separate GitHub Actions workflow can manually deploy to the droplet before auto-deploy is enabled

The new droplet workflow is intentionally conservative by default. Manual droplet deploys work once you add the droplet secrets, and automatic deploys on push only start after you set the repository variable `DO_DEPLOY_ENABLED=true`.

## Station feeds

MADIS is the primary feed for all seven stations. If it has no usable recent
temperature for VOR, AntFarm, Montecito, SM Pass, Parma, or Airport, the refresh
uses the station's public [MesoWest observation table](https://mesowest.utah.edu/cgi-bin/droman/meso_table_mesodyn.cgi?stn=SE068&unit=1&time=GMT&past=0&order=1)
over certificate-verified HTTPS. This fallback needs no API key. findU is only
queried for the actual CWOP call sign `KC6OYN`, not RAWS, SCE, or airport IDs.

On September 19, 2026, MADIS returned empty mesonets for the five RAWS/SCE
stations above. The NWS observation API was also stuck at 12:06–12:50 UTC,
while direct MesoWest tables contained observations at 14:47–15:06 UTC. This was
an upstream distribution outage, not five failed station instruments. A wider
MADIS time window and disabling its QC filter did not recover the observations.

The MesoWest parser checks station identity, UTC dates, explicit Celsius and m/s
column headings, finite physical bounds, and the same 0–60 minute freshness
limit as MADIS. It reads dated observation rows rather than the summary table
(which can contain older values), handles UTC midnight, and keeps wind/dewpoint
with the selected temperature report. RAWS sensor-height prefixes and missing
optional fields are supported. Compass winds are converted to degrees at the
table's 22.5-degree resolution. Existing MADIS station elevations are retained
because the observation tables omit elevation. Sources and original timestamps
are recorded in state/history and survive the last-good cache.

Malformed, stale, empty, or unavailable sources are logged and isolated to that
station. Regression fixtures in `tests/fixtures/` contain the header and first
three observations captured from each fallback station on September 19.
Maintenance note: [MesoWest](https://mesowest.utah.edu/) announces a December 31,
2026 sunset; migrate this fallback before then.

### La Cumbre

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

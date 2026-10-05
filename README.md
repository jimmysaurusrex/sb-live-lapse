# SB Live Lapse

This repo currently publishes the chart through GitHub Pages via [`.github/workflows/refresh.yml`](.github/workflows/refresh.yml). That path stays in place until the DigitalOcean droplet is live and you are happy with the cutover.

The DigitalOcean deployment assets live under [`deploy/digitalocean/`](deploy/digitalocean). They add a separate, timer-driven hosting path:

- the existing GitHub Pages workflow keeps publishing as best it can
- a droplet can refresh locally every 5 minutes with `systemd`
- a separate GitHub Actions workflow can manually deploy to the droplet before auto-deploy is enabled

The new droplet workflow is intentionally conservative by default. Manual droplet deploys work once you add the droplet secrets, and automatic deploys on push only start after you set the repository variable `DO_DEPLOY_ENABLED=true`.

## Station feeds

MADIS is the primary feed for all seven stations. If it has no usable recent
temperature for La Cumbre, VOR, AntFarm, Montecito, SM Pass, Parma, or Airport, the refresh
uses the station's public [MesoWest observation table](https://mesowest.utah.edu/cgi-bin/droman/meso_table_mesodyn.cgi?stn=SE068&unit=1&time=GMT&past=0&order=1)
over certificate-verified HTTPS. This fallback needs no API key. All current
stations use MADIS with MesoWest fallback; none query findU.

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

La Cumbre uses **`783SE`**, SCE La Cumbre Peak, in MADIS and MesoWest.
The [MesoWest station listing](https://mesowest.utah.edu/cgi-bin/droman/nearby_stns.cgi?stn=467SE)
reports an elevation of **3811 ft (1161.5928 m)**, used for chart placement and
lapse-rate calculations. The previous station was `KC6OYN` (displayed as
`KC60YN`, MADIS ID `AV377`).

On October 2, 2026, the MADIS query for `783SE` returned an empty mesonet while
the [MesoWest observation table](https://mesowest.utah.edu/cgi-bin/droman/meso_table_mesodyn.cgi?stn=783SE&unit=1&time=GMT&past=0&order=1)
contained observations. The fallback uses the same freshness, identity, units,
and physical-bounds checks as the other SCE stations. Its regression fixture
contains the header and first three observations captured that day.

New state and history use `783SE`. Cached readings from `KC6OYN` are excluded
from the current station cache, and older history keeps its original station
identity and elevation when charts are rebuilt. The legacy CWOP parser and
metadata remain available for historical compatibility and regression checks.

If both feeds fail, last-good readings for the same station can be retained for
up to 90 minutes; only observations aged 0–60 minutes are plotted as recent.
`provider` and `temp_source` retain the original service, station ID, HTTPS URL,
and observation time through the cache.

Run the feed regression checks with `python3 -m unittest discover -s tests -v`.

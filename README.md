# SB Live Lapse

This repo currently publishes the chart through GitHub Pages via [`.github/workflows/refresh.yml`](.github/workflows/refresh.yml). That path stays in place until the DigitalOcean droplet is live and you are happy with the cutover.

The DigitalOcean deployment assets live under [`deploy/digitalocean/`](deploy/digitalocean). They add a separate, timer-driven hosting path:

- the existing GitHub Pages workflow keeps publishing as best it can
- a droplet can refresh locally every 5 minutes with `systemd`
- a separate GitHub Actions workflow can manually deploy to the droplet before auto-deploy is enabled

The new droplet workflow is intentionally conservative by default. Manual droplet deploys work once you add the droplet secrets, and automatic deploys on push only start after you set the repository variable `DO_DEPLOY_ENABLED=true`.

## Station feeds

La Cumbre is queried as `KC6OYN` (letter O) and displayed as `KC60YN`.
When MADIS has no recent temperature, the refresh uses findU's CWOP XML feed.
It first tries HTTPS with certificate validation, then the same public endpoint
over HTTP if HTTPS fails or returns no usable report. This fallback sends only
the station call sign and lookback window, with no credentials. HTTP observations
are not authenticated in transit; the fallback is limited to findU, whose TLS
certificate was expired and missing its intermediate chain on September 14, 2026.
Feed failures and HTTP fallback use are logged. Responses must name the requested
station, and observation timestamps retain the existing 60-minute freshness limit.

Run the feed regression checks with `python3 -m unittest discover -s tests -v`.

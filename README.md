# SB Live Lapse

This repo currently publishes the chart through GitHub Pages via [`.github/workflows/refresh.yml`](.github/workflows/refresh.yml). That path stays in place until the DigitalOcean droplet is live and you are happy with the cutover.

The DigitalOcean deployment assets live under [`deploy/digitalocean/`](deploy/digitalocean). They add a separate, timer-driven hosting path:

- the existing GitHub Pages workflow keeps publishing as best it can
- a droplet can refresh locally every 5 minutes with `systemd`
- a separate GitHub Actions workflow can manually deploy to the droplet before auto-deploy is enabled

The new droplet workflow is intentionally conservative by default. Manual droplet deploys work once you add the droplet secrets, and automatic deploys on push only start after you set the repository variable `DO_DEPLOY_ENABLED=true`.

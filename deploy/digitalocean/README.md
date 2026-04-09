# DigitalOcean Droplet Plan

This deployment path is designed to run in parallel with GitHub Pages until you cut over.

## Layout

- repo checkout: `/opt/sb-live-lapse/repo`
- published web root: `/srv/sb-live-lapse/current`
- historical staged releases: `/srv/sb-live-lapse/releases`
- systemd env file: `/etc/sb-live-lapse.env`

## Files

- [`install.sh`](install.sh): one-time droplet bootstrap
- [`deploy.sh`](deploy.sh): update code on the droplet and run an immediate refresh
- [`run-refresh.sh`](run-refresh.sh): wrapper used by `systemd`
- [`sb-live-lapse-refresh.service`](sb-live-lapse-refresh.service): refresh unit
- [`sb-live-lapse-refresh.timer`](sb-live-lapse-refresh.timer): 5-minute timer
- [`Caddyfile`](Caddyfile): static site config template
- [`sb-live-lapse.env.example`](sb-live-lapse.env.example): environment template

## One-Time Bootstrap

Run this on the droplet as `root` or through `sudo`:

```bash
git clone https://github.com/jimmysaurusrex/sb-live-lapse.git /tmp/sb-live-lapse
cd /tmp/sb-live-lapse
sudo SB_LAPSE_SITE=:80 bash deploy/digitalocean/install.sh
```

If you already have a domain pointed at the droplet, replace `:80` with that hostname before installing. Example:

```bash
sudo SB_LAPSE_SITE=lapse.example.com bash deploy/digitalocean/install.sh
```

The installer:

- installs `git`, `python3`, `rsync`, and `caddy`
- creates the `sb-live-lapse` service user
- clones the repo to `/opt/sb-live-lapse/repo`
- writes `/etc/sb-live-lapse.env` if it does not exist
- installs the `systemd` unit and timer
- renders the Caddy config
- performs the first refresh immediately

## GitHub Deploy Workflow

The droplet deploy workflow supports two modes:

- manual `workflow_dispatch` as soon as the droplet secrets exist
- automatic deploy on `push` only after `DO_DEPLOY_ENABLED=true`

Configure all of the following before using it:

- repository secret: `DO_HOST`
- repository secret: `DO_USER`
- repository secret: `DO_SSH_PRIVATE_KEY`
- optional repository secret: `DO_PORT`

When you are ready for automatic deploys on every push to `main`, also add:

- repository variable: `DO_DEPLOY_ENABLED=true`

The workflow SSHes into the droplet and runs [`deploy.sh`](deploy.sh).

## Runtime Notes

- The droplet refresh path reads continuity state from local file URLs in `/opt/sb-live-lapse/repo`, not from GitHub Pages.
- Each successful refresh stages a new static release and atomically switches `/srv/sb-live-lapse/current`.
- Old staged releases are pruned automatically.
- GitHub Pages remains untouched until you decide to cut over.

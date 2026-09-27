# Deployment checklist

Everything in `deploy/` and `.github/workflows/` is real, validated configuration
(see the commits that added it), but nothing has actually been deployed: this
repo has no `git remote`, no domain, and no VPS. This is the human checklist to
turn that configuration into a live site. Nothing here has been done for you.

## 1. Create the GitHub repo and push

```
git remote add origin <your-repo-url>
git push -u origin master
```

Until this exists, `.github/workflows/*.yml` cannot run at all (GitHub Actions
only runs against a real GitHub repo), and there is nothing for GitHub Pages
to serve.

## 2. Buy a domain and a VPS

- Any domain registrar; any VPS provider with **2 vCPU / 4 GB RAM** minimum
  (design-plan.md section 13 - Redpanda alone wants ~1 GB). Check current
  pricing/free-tier terms yourself, they change often.
- Point a subdomain's A/AAAA record at the VPS's public IP, e.g.
  `api.yourdomain.com`. This is what `deploy/Caddyfile` currently has as the
  placeholder `api.example.com` - **edit that file** to your real subdomain
  before deploying, or Caddy will try (and fail) to get a TLS cert for a
  domain that doesn't point at it.
- GitHub Pages is served over HTTPS, so the gateway must be `wss://`
  (design-plan.md 13's "mixed content" note) - this is exactly what Caddy's
  automatic TLS is for.

## 3. Prepare the VPS

On the VPS (once, by hand):

```
git clone <your-repo-url> /opt/oddspulse
cd /opt/oddspulse
docker compose -f deploy/docker-compose.yml -f deploy/docker-compose.prod.yml up -d
```

This is also the command `deploy.yml`'s automated deploy runs later - doing it
by hand once first confirms the VPS itself, Docker, and DNS all actually work
before wiring up automation.

Grafana (`http://<vps-ip>:3000`, not proxied by Caddy - see
`deploy/docker-compose.prod.yml`) is reachable only via the VPS's own IP or an
SSH tunnel, never publicly. Change its default `admin`/`admin` login on first
visit (`GF_SECURITY_ADMIN_PASSWORD` env var, or through the UI) - this repo's
local testing used the default because it was a throwaway container on a
laptop, real deploys should not.

## 4. GitHub repo configuration

In the repo's Settings:

- **Settings -> Pages**: set Source to "GitHub Actions".
- **Settings -> Secrets and variables -> Actions -> Variables**: add
  `VITE_WS_URL` = `wss://api.yourdomain.com/ws` (used by `pages.yml`).
- **Settings -> Secrets and variables -> Actions -> Secrets** (only needed for
  `deploy.yml`, optional): `VPS_HOST`, `VPS_USER`, `VPS_SSH_KEY` (a private
  key authorized on the VPS for that user).
- `images.yml` needs no extra secrets - it uses the repo's built-in
  `GITHUB_TOKEN` to push to GHCR. The VPS then needs `docker login ghcr.io`
  (a GHCR read token) done once by hand so `docker compose pull` can pull
  those images, unless the images are made public in GHCR's package
  settings.
- `web/vite.config.ts`'s `base` (`/polymorph/`) and
  `web/src/pages/About.svelte`'s `GITHUB_URL`
  (`https://github.com/nawat-john/polymorph`) must match the repo
  name/owner - update both if the repo is renamed.
- `deploy/docker-compose.prod.yml`'s `GW_ALLOWED_ORIGINS`
  (`https://nawatpim.com`, plus `http://` until HTTPS is enforced on the
  domain) must match the Pages site's origin, or every browser WebSocket to
  the gateway is rejected.

## 5. First real deploy

Push a tag once `images.yml`/`deploy.yml` should run:

```
git tag v0.1.0
git push origin v0.1.0
```

`images.yml` builds and pushes images to GHCR; `deploy.yml` (if its secrets
are set) then SSHes into the VPS and runs
`docker compose -f docker-compose.yml -f docker-compose.prod.yml pull && up -d`.
Pushing to `master`/`main` under `web/**` separately triggers `pages.yml`.

## 6. Verify

- `https://api.yourdomain.com/healthz` -> `200`
- `wss://api.yourdomain.com/ws` -> the frontend's System Stats badge should
  read LIVE, not REPLAY
- The GitHub Pages URL loads and, if the VPS is ever down, still shows the
  REPLAY badge and real recorded data within ~3 seconds (this fallback is
  already verified locally - see the Phase 5 report)

## What is already done vs. still manual

| Already done (this repo) | Still needs a human |
|---|---|
| `deploy/docker-compose.prod.yml`, validated with `docker compose config` | Buying the domain and VPS |
| `deploy/Caddyfile`, validated with `caddy validate` and a real local run | Editing the Caddyfile's placeholder domain |
| `deploy/prometheus/`, `deploy/grafana/` - actually run locally against the live stack | Changing Grafana's default password on the real deploy |
| `.github/workflows/pages.yml`, `images.yml`, `deploy.yml` - YAML validated | `git remote add origin`, repo secrets/variables, enabling Pages |
| Recorder + replay export + frontend fallback, verified live end-to-end | Recording a longer/more eventful replay clip (design-plan.md Phase 7) once the VPS is running continuously |

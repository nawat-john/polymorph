# ADR-005: Host the frontend on GitHub Pages, with a replay fallback

## Context

The frontend (§7) is meant to be the thing a portfolio reviewer actually opens. It
needs to be reachable for free, indefinitely, without OddsPulse's own operator paying
for hosting or keeping a server awake. The backend (§13), by contrast, runs as
`docker-compose` on a single small VPS and is not expected to have the same
uptime guarantees as a static site — a cheap VPS can be stopped to save cost, a
free-tier container host can sleep the instance, and the Polymarket connection itself
can be interrupted (§16).

A reviewer opening the demo link while the backend happens to be down, asleep, or
network-blocked from `wss://` would otherwise see a blank page — the worst possible
first impression for a project whose whole point is to demonstrate a working system.

## Decision

- Deploy the static frontend build to **GitHub Pages** via `actions/deploy-pages`
  (§13, §14 `pages.yml`), fully decoupled from the backend's own lifecycle.
- Because GitHub Pages is HTTPS-only, the browser will refuse plain `ws://`, so the
  gateway must be reachable at `wss://` through a real TLS-terminating reverse proxy
  (Caddy, §13) on its own subdomain.
- Build a **Replay mode** (§8) as a first-class fallback, not an afterthought: on load
  the page tries `LiveSource.connect()` with a 3s timeout; on failure it switches to
  `ReplaySource`, which streams pre-recorded `.ndjson.gz` ticks from
  `web/public/replay/` (produced by the `recorder` service and
  `scripts/replay-export.sh`) at the original relative timing, with a visible
  "REPLAY — recorded <date>" badge and a "Try live" button.
- Both sources implement the same `FeedSource` interface (§7) so UI components never
  know which mode they're in.

## Consequences

- The demo link works even when the entire backend is off — satisfying the portfolio
  checklist's "opens and shows moving data within 3 seconds, always" (§17).
- Replay data goes stale between recordings; the badge exists specifically so this is
  never presented as live.
- Two code paths (`LiveSource`, `ReplaySource`) must be kept behaviorally consistent
  behind the same interface, which is extra surface area but is exercised by CI's
  frontend build/typecheck job once `web/` exists.
- The frontend repo/pages deployment and the backend VPS deployment are independent:
  a broken backend deploy cannot take down the demo page, and a Pages outage cannot
  take down the backend.

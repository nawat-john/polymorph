// design-plan.md section 7: VITE_WS_URL, default matching the local gateway
// (deploy/docker-compose.yml maps gateway-1's :8080 to the host's :8081).
export const WS_URL: string = import.meta.env.VITE_WS_URL ?? 'ws://localhost:8081/ws'

// The gateway serves GET /markets on the same host:port as /ws (cmd/gateway/
// marketcache.go); derive its http(s) base from WS_URL rather than adding a
// second env var for what is always the same origin.
export const API_BASE: string = WS_URL.replace(/^ws/, 'http').replace(/\/ws$/, '')

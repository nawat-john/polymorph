// Phase 0 probe: fetch a few real Gamma markets and capture a short sample of the CLOB market WebSocket.
// Zero dependencies; needs Node 22+ (global WebSocket, fetch).
//
//   node scripts/probe-polymarket.mjs [outDir] [captureSeconds]
//
// Writes into outDir (default internal/polymarket/testdata):
//   gamma-markets.sample.json        raw Gamma /markets response (verbatim, N markets)
//   clobws-market.sample.ndjson      one raw WS text frame per line, verbatim, in arrival order
//   probe-summary.json               what was observed (event type counts, PONG seen, timings)
import { mkdir, writeFile } from "node:fs/promises";

const GAMMA = "https://gamma-api.polymarket.com";
const WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
const outDir = process.argv[2] ?? "internal/polymarket/testdata";
const seconds = Number(process.argv[3] ?? 20);
const MAX_FRAMES = 60;

await mkdir(outDir, { recursive: true });

// 1. Gamma: most active markets by 24h volume.
const url = `${GAMMA}/markets?active=true&closed=false&limit=3&order=volume24hr&ascending=false`;
const res = await fetch(url);
if (!res.ok) throw new Error(`gamma: HTTP ${res.status}`);
const markets = await res.json();
await writeFile(`${outDir}/gamma-markets.sample.json`, JSON.stringify(markets, null, 2) + "\n");
// clobTokenIds is a JSON-encoded string inside the JSON object.
const assets = markets.flatMap((m) => JSON.parse(m.clobTokenIds));
console.log(`gamma ok: ${markets.length} markets, ${assets.length} assets`);

// 2. CLOB WS: subscribe, ping every 10 s, record frames verbatim.
const frames = [];
const summary = { ws_url: WS_URL, captured_at: new Date().toISOString(), event_types: {}, pong_seen: false };
await new Promise((resolve, reject) => {
  const ws = new WebSocket(WS_URL);
  let ping;
  const finish = () => { clearInterval(ping); try { ws.close(); } catch {} resolve(); };
  const timer = setTimeout(finish, seconds * 1000);
  ws.onopen = () => {
    ws.send(JSON.stringify({ assets_ids: assets, type: "market" }));
    ping = setInterval(() => ws.send("PING"), 10_000);
  };
  ws.onmessage = (ev) => {
    const text = String(ev.data);
    if (text === "PONG") { summary.pong_seen = true; return; }
    if (frames.length < MAX_FRAMES) frames.push(text);
    try {
      const parsed = JSON.parse(text);
      for (const e of Array.isArray(parsed) ? parsed : [parsed]) {
        summary.event_types[e.event_type] = (summary.event_types[e.event_type] ?? 0) + 1;
      }
    } catch { summary.event_types["<non-json>"] = (summary.event_types["<non-json>"] ?? 0) + 1; }
    if (frames.length >= MAX_FRAMES) { clearTimeout(timer); finish(); }
  };
  ws.onerror = (e) => { clearTimeout(timer); reject(new Error(`ws error: ${e.message ?? e.type}`)); };
});

summary.frames_saved = frames.length;
await writeFile(`${outDir}/clobws-market.sample.ndjson`, frames.join("\n") + "\n");
await writeFile(`${outDir}/probe-summary.json`, JSON.stringify(summary, null, 2) + "\n");
console.log(JSON.stringify(summary));

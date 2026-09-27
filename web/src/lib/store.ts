// Shared, feed-agnostic application state. Components read these stores and
// never touch the active FeedSource directly (design-plan.md section 7:
// "the UI does not know where the data comes from"). start() tries
// LiveSource.connect() with a 3s timeout and falls back to ReplaySource on
// failure, per design-plan.md section 8.
import { derived, get, writable } from 'svelte/store'
import { LiveSource } from '../feed/live-source'
import { ReplaySource } from '../feed/replay-source'
import type { Alert, ConnStatus, FeedSource, Market, SysStats, Tick, TopEntry } from '../feed/types'
import { WS_URL } from './config'
import { fetchMarkets } from './api'

const LIVE_CONNECT_TIMEOUT_MS = 3000

export type Mode = 'live' | 'replay'
export const mode = writable<Mode>('live')
export const replayRecordedAt = writable<string | null>(null)

export const status = writable<ConnStatus>('connecting')
export const sys = writable<SysStats | null>(null)
export const top = writable<TopEntry[]>([])
export const alerts = writable<Alert[]>([])
export const markets = writable<Market[]>([])

/** asset id -> latest Tick. Updated at most once per animation frame
 * (design-plan.md section 7: "batch incoming ticks ... render via
 * requestAnimationFrame, not per message"), not once per WS message. */
export const ticks = writable<Map<string, Tick>>(new Map())

/** asset ids touched by the most recent rAF flush, for a "flash on update"
 * effect that doesn't require re-scanning the whole ticks map. */
export const flashed = writable<string[]>([])

let pending = new Map<string, Tick>()
let rafScheduled = false

function scheduleFlush(): void {
  if (rafScheduled) return
  rafScheduled = true
  requestAnimationFrame(() => {
    rafScheduled = false
    if (pending.size === 0) return
    const batch = pending
    pending = new Map()
    ticks.update((m) => {
      for (const [id, t] of batch) m.set(id, t)
      return m
    })
    flashed.set([...batch.keys()])
  })
}

/** Attaches every store update to a FeedSource's events - shared by whichever
 * source (LiveSource or ReplaySource) is currently active, so switching
 * between them means constructing a new source and wiring it, not touching
 * any component. */
function wire(src: FeedSource): void {
  src.on('ticks', (list) => {
    for (const t of list) pending.set(t.a, t)
    scheduleFlush()
  })
  src.on('top', (list) => top.set(list))
  src.on('alert', (a) => alerts.update((prev) => [a, ...prev].slice(0, 50)))
  src.on('sys', (s) => sys.set(s))
  src.on('status', (s) => status.set(s))
}

/** market_id/asset_id lookups, derived from `markets` so components can show
 * real questions/outcomes instead of bare token ids. */
export interface AssetInfo {
  market: Market
  outcome: string
}
export const assetByID = derived(markets, ($markets) => {
  const m = new Map<string, AssetInfo>()
  for (const mkt of $markets) {
    mkt.clob_token_ids.forEach((id, i) => {
      m.set(id, { market: mkt, outcome: mkt.outcomes?.[i] ?? `Outcome ${i + 1}` })
    })
  }
  return m
})

// Guard against subscribing to more assets than the gateway allows per
// client (cmd/gateway Config.MaxSubs, GW_MAX_SUBS, default 500) - top/alerts/
// sys are 3 more subscriptions, so leave headroom.
const MAX_ASSET_SUBS = 490

function subscribeChannels(src: FeedSource, mkts: Market[]): void {
  src.subscribe('sys')
  src.subscribe('top')
  src.subscribe('alerts')
  const assetIDs = mkts.flatMap((m) => m.clob_token_ids).slice(0, MAX_ASSET_SUBS)
  if (assetIDs.length > 0) src.subscribe('asset', assetIDs)
}

function withTimeout<T>(p: Promise<T>, ms: number): Promise<T> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('timeout')), ms)
    p.then(
      (v) => {
        clearTimeout(timer)
        resolve(v)
      },
      (err) => {
        clearTimeout(timer)
        reject(err)
      },
    )
  })
}

let active: FeedSource | null = null

function resetFeedState(): void {
  ticks.set(new Map())
  top.set([])
  alerts.set([])
  sys.set(null)
}

/** Tries LiveSource.connect() with a 3s timeout; on failure, tears it down
 * and returns false without touching `active` (design-plan.md section 8). */
async function tryConnectLive(): Promise<LiveSource | null> {
  const live = new LiveSource(WS_URL)
  wire(live)
  try {
    await withTimeout(live.connect(), LIVE_CONNECT_TIMEOUT_MS)
    return live
  } catch {
    live.close()
    return null
  }
}

async function goLive(live: LiveSource): Promise<void> {
  resetFeedState()
  active = live
  mode.set('live')
  replayRecordedAt.set(null)
  const mkts = await fetchMarkets().catch(() => [] as Market[])
  markets.set(mkts)
  subscribeChannels(live, mkts)
}

async function goReplay(): Promise<void> {
  const replay = new ReplaySource()
  wire(replay)
  resetFeedState()
  active = replay
  await replay.connect()
  mode.set('replay')
  replayRecordedAt.set(replay.manifest?.recorded_at ?? null)
  const mkts = await replay.loadMarkets().catch(() => [] as Market[])
  markets.set(mkts)
}

/** Connects to the gateway with a 3s timeout, loads market metadata, and
 * subscribes to every channel the UI needs; falls back to ReplaySource on
 * failure (design-plan.md section 8). Call once at startup. */
export async function start(): Promise<void> {
  const live = await tryConnectLive()
  if (live) {
    await goLive(live)
  } else {
    await goReplay()
  }
}

/** Sets replay playback speed (design-plan.md section 8: 1x/5x/20x). No-op
 * in live mode. */
export function setReplaySpeed(speed: number): void {
  if (active instanceof ReplaySource) active.setSpeed(speed)
}

/** Lets a component (e.g. MarketDetail) subscribe/unsubscribe extra asset
 * ids on whichever source is currently active, without knowing which one
 * that is. A no-op while `active` is unset (before start() resolves). */
export function subscribeAsset(ids: string[]): void {
  active?.subscribe('asset', ids)
}
export function unsubscribeAsset(ids: string[]): void {
  active?.unsubscribe('asset', ids)
}

/** "Try live" button (design-plan.md section 8): attempts to reconnect to
 * LiveSource without disturbing replay playback unless it actually
 * succeeds. */
export async function tryLive(): Promise<boolean> {
  if (get(mode) === 'live') return true
  const live = await tryConnectLive()
  if (!live) return false
  ;(active as ReplaySource | null)?.close()
  await goLive(live)
  return true
}

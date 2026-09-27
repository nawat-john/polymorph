// Shared, feed-agnostic application state. Components read these stores and
// never touch `source` directly, so swapping LiveSource for a Phase 5
// ReplaySource (feed/) means changing only the two lines below, per
// design-plan.md section 7's "the UI does not know where the data comes
// from".
import { derived, writable } from 'svelte/store'
import { LiveSource } from '../feed/live-source'
import type { Alert, ConnStatus, Market, SysStats, Tick, TopEntry } from '../feed/types'
import { WS_URL } from './config'
import { fetchMarkets } from './api'

export const source = new LiveSource(WS_URL)

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

source.on('ticks', (list) => {
  for (const t of list) pending.set(t.a, t)
  scheduleFlush()
})
source.on('top', (list) => top.set(list))
source.on('alert', (a) => alerts.update((prev) => [a, ...prev].slice(0, 50)))
source.on('sys', (s) => sys.set(s))
source.on('status', (s) => status.set(s))

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

/** Connects to the gateway, loads market metadata, and subscribes to every
 * channel the UI needs. Call once at startup. */
export async function start(): Promise<void> {
  const [mkts] = await Promise.all([fetchMarkets().catch(() => [] as Market[]), source.connect()])
  markets.set(mkts)
  source.subscribe('sys')
  source.subscribe('top')
  source.subscribe('alerts')
  const assetIDs = mkts.flatMap((m) => m.clob_token_ids).slice(0, MAX_ASSET_SUBS)
  if (assetIDs.length > 0) source.subscribe('asset', assetIDs)
}

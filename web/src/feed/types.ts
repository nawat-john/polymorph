// Shared model/protocol types (design-plan.md sections 5 and 6), mirrored
// from the Go structs in internal/model and internal/wsproto. Field names
// match the real wire JSON exactly (short keys included) so no mapping layer
// is needed between the gateway and the frontend.

/** pm.ticks (internal/model/tick.go). */
export interface Tick {
  v: number
  a: string // asset id
  m: string // market id
  p: number // last/mid price
  b: number // best bid
  k: number // best ask
  c1m: number // pp change over 1 minute
  c5m: number // pp change over 5 minutes
  vol: number // 5-minute volatility
  seq: number
  rts: number // ingestor receive time (ms epoch)
  pts: number // processor produce time (ms epoch)
}

/** pm.alerts (internal/model/alert.go). */
export interface Alert {
  v: number
  a: string
  m: string
  q: string // question
  outcome: string
  from: number
  to: number
  window_s: number
  ts: number
}

/** One row of the "top" channel (internal/model/top.go TopEntry). */
export interface TopEntry {
  a: string
  c5m: number
}

/** "sys" channel payload (internal/wsproto SysStats). */
export interface SysStats {
  clients: number
  in_eps: number
  out_mps: number
  p99_ms: number
}

/** GET /markets response row (internal/model/market.go Market). */
export interface Market {
  v: number
  market_id: string
  question: string
  slug: string
  end_date?: string
  category?: string
  tags?: string[]
  volume: number
  active: boolean
  closed: boolean
  outcomes?: string[]
  clob_token_ids: string[]
}

/** Connection status reported on the 'status' event. */
export type ConnStatus = 'connecting' | 'open' | 'closed'

export type Channel = 'asset' | 'top' | 'alerts' | 'sys'

export type FeedEvent = 'ticks' | 'alert' | 'top' | 'sys' | 'status'

export type Handler<T = unknown> = (data: T) => void

/**
 * Data source abstraction (design-plan.md section 7): components subscribe
 * to channels and register handlers without knowing whether the data comes
 * from a live WebSocket (LiveSource, this phase) or a recorded replay file
 * (ReplaySource, Phase 5).
 */
export interface FeedSource {
  connect(): Promise<void>
  subscribe(ch: Channel, ids?: string[]): void
  unsubscribe(ch: Channel, ids?: string[]): void
  on(event: 'ticks', cb: Handler<Tick[]>): void
  on(event: 'alert', cb: Handler<Alert>): void
  on(event: 'top', cb: Handler<TopEntry[]>): void
  on(event: 'sys', cb: Handler<SysStats>): void
  on(event: 'status', cb: Handler<ConnStatus>): void
}

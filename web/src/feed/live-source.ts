import type {
  Alert,
  Channel,
  ConnStatus,
  FeedEvent,
  FeedSource,
  Handler,
  SysStats,
  Tick,
  TopEntry,
} from './types'

// design-plan.md section 6: server -> client message envelope. Only the
// fields relevant to `t` are populated.
interface ServerMsg {
  t: string
  d?: unknown
  server?: string
  proto?: number
  code?: string
  msg?: string
  t0?: number
  srv?: number
}

// Reconnect backoff (design-plan.md 4.1's ingestor policy, reused here for
// the browser's own reconnect): exponential, 500ms -> capped at 30s, with
// jitter so many tabs reconnecting at once don't hammer the gateway at the
// same instant.
const BASE_DELAY_MS = 500
const MAX_DELAY_MS = 30_000

// App-level ping (design-plan.md section 6 client->server ops). Separate
// from the transport-level WS ping the gateway itself sends every 30s.
const PING_INTERVAL_MS = 20_000

const WS_OPEN = 1 // WebSocket.OPEN, hardcoded to avoid depending on a global constant in tests.

/**
 * Real WebSocket implementation of FeedSource (design-plan.md section 7),
 * talking the protocol in internal/wsproto exactly: sends
 * {op:"sub"/"unsub"/"ping", ch, ids?}, handles hello/snap/ticks/top/alert/
 * sys/pong/err. Reconnects with backoff+jitter and re-sends every tracked
 * subscription once the new connection says hello.
 */
export class LiveSource implements FeedSource {
  private readonly url: string
  private ws: WebSocket | null = null
  private readonly handlers: { [K in FeedEvent]: Handler<any>[] } = {
    ticks: [],
    alert: [],
    top: [],
    sys: [],
    status: [],
  }
  // Channel -> subscribed ids. An entry with an empty set means "subscribed
  // to the whole channel" (top/alerts/sys carry no ids).
  private readonly subs = new Map<Channel, Set<string>>()
  private attempt = 0
  private pingTimer: ReturnType<typeof setInterval> | undefined
  private reconnectTimer: ReturnType<typeof setTimeout> | undefined
  private closedByUser = false
  private helloWaiters: Array<() => void> = []

  constructor(url: string) {
    this.url = url
  }

  connect(): Promise<void> {
    this.closedByUser = false
    return new Promise((resolve) => {
      this.helloWaiters.push(resolve)
      this.open()
    })
  }

  /** Stops reconnecting and closes the current socket, if any. */
  close(): void {
    this.closedByUser = true
    clearTimeout(this.reconnectTimer)
    this.stopPing()
    this.ws?.close()
  }

  subscribe(ch: Channel, ids?: string[]): void {
    let set = this.subs.get(ch)
    if (!set) {
      set = new Set()
      this.subs.set(ch, set)
    }
    if (ids && ids.length > 0) {
      for (const id of ids) set.add(id)
      this.send({ op: 'sub', ch, ids })
    } else {
      this.send({ op: 'sub', ch })
    }
  }

  unsubscribe(ch: Channel, ids?: string[]): void {
    const set = this.subs.get(ch)
    if (ids && ids.length > 0) {
      set?.forEach((id) => {
        if (ids.includes(id)) set.delete(id)
      })
      this.send({ op: 'unsub', ch, ids })
    } else {
      this.subs.delete(ch)
      this.send({ op: 'unsub', ch })
    }
  }

  on(event: 'ticks', cb: Handler<Tick[]>): void
  on(event: 'alert', cb: Handler<Alert>): void
  on(event: 'top', cb: Handler<TopEntry[]>): void
  on(event: 'sys', cb: Handler<SysStats>): void
  on(event: 'status', cb: Handler<ConnStatus>): void
  on(event: FeedEvent, cb: Handler<any>): void {
    this.handlers[event].push(cb)
  }

  private open(): void {
    this.emit('status', 'connecting')
    const ws = new WebSocket(this.url)
    this.ws = ws
    ws.onopen = () => {
      this.attempt = 0
    }
    ws.onmessage = (ev) => this.handleMessage(ev.data as string)
    ws.onclose = () => this.handleClose()
    ws.onerror = () => {
      // onclose always follows onerror for a browser WebSocket; nothing
      // extra to do here.
    }
  }

  private handleMessage(raw: string): void {
    let msg: ServerMsg
    try {
      msg = JSON.parse(raw)
    } catch {
      return // malformed frame; ignore per gateway's own leniency
    }
    switch (msg.t) {
      case 'hello':
        this.emit('status', 'open')
        this.resubscribeAll()
        this.startPing()
        this.resolveHello()
        break
      case 'snap':
      case 'ticks':
        this.emit('ticks', (msg.d as Tick[]) ?? [])
        break
      case 'top':
        this.emit('top', (msg.d as TopEntry[]) ?? [])
        break
      case 'alert':
        this.emit('alert', msg.d as Alert)
        break
      case 'sys':
        this.emit('sys', msg.d as SysStats)
        break
      case 'pong':
      case 'err':
        // pong: round-trip echo, informational only, no UI consumes it yet.
        // err (e.g. code "too_many_subs"): nothing to retry, this client
        // does not send subscriptions beyond GW_MAX_SUBS.
        break
    }
  }

  private handleClose(): void {
    this.stopPing()
    this.emit('status', 'closed')
    if (this.closedByUser) return
    const delay = Math.min(MAX_DELAY_MS, BASE_DELAY_MS * 2 ** this.attempt)
    const jitter = delay * (0.5 + Math.random() * 0.5) // 50%-100% of delay
    this.attempt++
    this.reconnectTimer = setTimeout(() => this.open(), jitter)
  }

  private resubscribeAll(): void {
    for (const [ch, ids] of this.subs) {
      if (ids.size > 0) this.send({ op: 'sub', ch, ids: [...ids] })
      else this.send({ op: 'sub', ch })
    }
  }

  private startPing(): void {
    this.stopPing()
    this.pingTimer = setInterval(() => this.send({ op: 'ping', t: Date.now() }), PING_INTERVAL_MS)
  }

  private stopPing(): void {
    clearInterval(this.pingTimer)
    this.pingTimer = undefined
  }

  private resolveHello(): void {
    const waiters = this.helloWaiters
    this.helloWaiters = []
    waiters.forEach((resolve) => resolve())
  }

  private send(op: { op: string; ch?: string; ids?: string[]; t?: number }): void {
    if (this.ws && this.ws.readyState === WS_OPEN) {
      this.ws.send(JSON.stringify(op))
    }
    // Not connected: silently dropped. Subscriptions are tracked in
    // this.subs regardless and re-sent in full once the socket reconnects
    // and says hello (resubscribeAll) - pings are simply skipped meanwhile.
  }

  private emit(event: FeedEvent, data: unknown): void {
    for (const h of this.handlers[event]) h(data)
  }
}

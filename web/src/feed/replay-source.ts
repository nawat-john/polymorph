import type { Alert, Channel, ConnStatus, FeedEvent, FeedSource, Handler, Market, SysStats, Tick, TopEntry } from './types'

/** web/public/replay/manifest.json (design-plan.md section 8). */
export interface Manifest {
  recorded_at: string
  duration_s: number
  files: string[]
  markets: string
  benchmark: string
}

interface ReplayEvent {
  ts: number // original epoch ms (Tick.pts or Alert.ts)
  kind: 'tick' | 'alert'
  data: Tick | Alert
}

// One NDJSON line as written by cmd/recorder (record{T, D} in cmd/recorder/main.go).
interface RecordLine {
  t: 'tick' | 'alert'
  d: Tick | Alert
}

// How often the playback loop checks for events whose scheduled time has
// arrived. A single interval rather than one setTimeout per event, so
// playback cost does not grow with how densely events are packed.
const PUMP_INTERVAL_MS = 50

// How often a synthetic 'top' list is recomputed from replayed ticks and
// emitted, matching the processor's own ~1s cadence (design-plan.md 4.2).
// Real pm.top is not recorded (cmd/recorder only captures pm.ticks/
// pm.alerts, design-plan.md 4.4), so this is derived here from the real
// recorded ticks rather than fabricated.
const TOP_EMIT_INTERVAL_MS = 1000
const TOP_LIST_SIZE = 50

/**
 * Replay-file implementation of FeedSource (design-plan.md sections 7-8):
 * loads a recorded manifest + gzip NDJSON files, decompresses them with the
 * browser's native DecompressionStream, and replays events at their
 * original relative spacing (speed-adjustable), looping on completion.
 * subscribe()/unsubscribe() are no-ops: a replay clip is small enough
 * (<5MB, design-plan.md 4.4) to just broadcast every recorded event, same
 * as the components already do for LiveSource's snapshot-on-subscribe.
 */
export class ReplaySource implements FeedSource {
  private readonly baseUrl: string
  manifest: Manifest | null = null

  private readonly handlers: { [K in FeedEvent]: Handler<any>[] } = {
    ticks: [],
    alert: [],
    top: [],
    sys: [],
    status: [],
  }

  private events: ReplayEvent[] = []
  private playIndex = 0
  private originStart = 0
  private elapsedBase = 0
  private wallBase = 0
  private speed = 1
  private pumpTimer: ReturnType<typeof setInterval> | undefined
  private lastTopEmit = 0
  private readonly latestTicks = new Map<string, Tick>()

  constructor(baseUrl = `${import.meta.env.BASE_URL}replay/`) {
    this.baseUrl = baseUrl
  }

  async connect(): Promise<void> {
    this.emit('status', 'connecting')
    const res = await fetch(`${this.baseUrl}manifest.json`)
    if (!res.ok) throw new Error(`GET ${this.baseUrl}manifest.json: ${res.status}`)
    this.manifest = (await res.json()) as Manifest

    const events: ReplayEvent[] = []
    for (const file of this.manifest.files) {
      events.push(...(await this.loadFile(`${this.baseUrl}${file}`)))
    }
    events.sort((a, b) => a.ts - b.ts)
    this.events = events

    this.emit('status', 'open')
    this.startPlayback()
  }

  /** Loads the market metadata bundled alongside the replay clip (manifest.markets), the replay-mode substitute for GET /markets. */
  async loadMarkets(): Promise<Market[]> {
    const name = this.manifest?.markets ?? 'markets.json'
    const res = await fetch(`${this.baseUrl}${name}`)
    if (!res.ok) throw new Error(`GET ${this.baseUrl}${name}: ${res.status}`)
    return (await res.json()) as Market[]
  }

  /** Sets playback speed (design-plan.md section 8: 1x/5x/20x). */
  setSpeed(speed: number): void {
    this.elapsedBase = this.currentElapsed()
    this.wallBase = performance.now()
    this.speed = speed
  }

  /** Stops playback. Not part of FeedSource - called when switching back to LiveSource. */
  close(): void {
    clearInterval(this.pumpTimer)
    this.pumpTimer = undefined
  }

  // subscribe/unsubscribe: no-op (see class doc).
  subscribe(_ch: Channel, _ids?: string[]): void {}
  unsubscribe(_ch: Channel, _ids?: string[]): void {}

  on(event: 'ticks', cb: Handler<Tick[]>): void
  on(event: 'alert', cb: Handler<Alert>): void
  on(event: 'top', cb: Handler<TopEntry[]>): void
  on(event: 'sys', cb: Handler<SysStats>): void
  on(event: 'status', cb: Handler<ConnStatus>): void
  on(event: FeedEvent, cb: Handler<any>): void {
    this.handlers[event].push(cb)
  }

  private async loadFile(url: string): Promise<ReplayEvent[]> {
    const res = await fetch(url)
    if (!res.ok || !res.body) throw new Error(`GET ${url}: ${res.status}`)
    // Some static file servers (vite's dev/preview server among them, via
    // sirv's "pre-gzipped asset" convention for .gz files) tag a .gz
    // response with a real Content-Encoding: gzip header, which makes the
    // browser's fetch() transparently decompress the body - res.body is
    // then already plain text, and re-running DecompressionStream on it
    // fails (found via real Chrome testing against `npm run dev`; GitHub
    // Pages itself does not do this, so production still hits the
    // DecompressionStream path below).
    const alreadyDecoded = res.headers.get('content-encoding')?.includes('gzip') ?? false
    const text = alreadyDecoded
      ? await res.text()
      : await new Response(res.body.pipeThrough(new DecompressionStream('gzip'))).text()

    const out: ReplayEvent[] = []
    for (const line of text.split('\n')) {
      if (!line) continue
      const rec = JSON.parse(line) as RecordLine
      const ts = rec.t === 'tick' ? (rec.d as Tick).pts : (rec.d as Alert).ts
      out.push({ ts, kind: rec.t, data: rec.d })
    }
    return out
  }

  private startPlayback(): void {
    this.playIndex = 0
    this.originStart = this.events[0]?.ts ?? 0
    this.elapsedBase = 0
    this.wallBase = performance.now()
    this.lastTopEmit = 0
    clearInterval(this.pumpTimer)
    this.pumpTimer = setInterval(() => this.pump(), PUMP_INTERVAL_MS)
  }

  private currentElapsed(): number {
    return this.elapsedBase + (performance.now() - this.wallBase) * this.speed
  }

  private pump(): void {
    if (this.events.length === 0) return
    const elapsed = this.currentElapsed()
    while (this.playIndex < this.events.length && this.events[this.playIndex].ts - this.originStart <= elapsed) {
      this.emitEvent(this.events[this.playIndex])
      this.playIndex++
    }
    if (this.playIndex >= this.events.length) {
      // Loop (design-plan.md section 8).
      this.playIndex = 0
      this.originStart = this.events[0].ts
      this.elapsedBase = 0
      this.wallBase = performance.now()
    }
  }

  private emitEvent(ev: ReplayEvent): void {
    if (ev.kind === 'tick') {
      const tick = ev.data as Tick
      this.latestTicks.set(tick.a, tick)
      this.emit('ticks', [tick])
      this.maybeEmitTop()
    } else {
      this.emit('alert', ev.data as Alert)
    }
  }

  private maybeEmitTop(): void {
    const now = performance.now()
    if (now - this.lastTopEmit < TOP_EMIT_INTERVAL_MS) return
    this.lastTopEmit = now
    const top: TopEntry[] = [...this.latestTicks.values()]
      .sort((a, b) => Math.abs(b.c5m) - Math.abs(a.c5m))
      .slice(0, TOP_LIST_SIZE)
      .map((t) => ({ a: t.a, c5m: t.c5m }))
    this.emit('top', top)
  }

  private emit(event: FeedEvent, data: unknown): void {
    for (const h of this.handlers[event]) h(data)
  }
}

import { beforeEach, describe, expect, it, vi } from 'vitest'
import { LiveSource } from './live-source'
import type { Tick } from './types'

// Minimal fake WebSocket: enough surface for LiveSource (onopen/onmessage/
// onclose/onerror, send, readyState, close) plus test helpers to drive it.
class FakeWebSocket {
  static instances: FakeWebSocket[] = []
  static OPEN = 1

  readyState = 0
  sent: string[] = []
  onopen: (() => void) | null = null
  onmessage: ((ev: { data: string }) => void) | null = null
  onclose: (() => void) | null = null
  onerror: (() => void) | null = null

  constructor(public url: string) {
    FakeWebSocket.instances.push(this)
  }

  send(data: string): void {
    this.sent.push(data)
  }

  close(): void {
    this.readyState = 3
    this.onclose?.()
  }

  // --- test helpers, not part of the real WebSocket API ---
  serverOpen(): void {
    this.readyState = 1
    this.onopen?.()
  }

  serverSend(obj: unknown): void {
    this.onmessage?.({ data: JSON.stringify(obj) })
  }

  serverHello(): void {
    this.serverOpen()
    this.serverSend({ t: 'hello', server: 'gw-1', proto: 1 })
  }

  serverDrop(): void {
    this.readyState = 3
    this.onclose?.()
  }

  lastSent(): unknown {
    return JSON.parse(this.sent[this.sent.length - 1])
  }
}

function currentWs(): FakeWebSocket {
  return FakeWebSocket.instances[FakeWebSocket.instances.length - 1]
}

beforeEach(() => {
  FakeWebSocket.instances = []
  vi.stubGlobal('WebSocket', FakeWebSocket)
  vi.useFakeTimers()
})

describe('LiveSource', () => {
  it('resolves connect() once the server says hello', async () => {
    const src = new LiveSource('ws://x/ws')
    const connected = src.connect()
    expect(FakeWebSocket.instances).toHaveLength(1)
    currentWs().serverHello()
    await expect(connected).resolves.toBeUndefined()
  })

  it('sends a sub message and re-sends it after reconnect', async () => {
    const src = new LiveSource('ws://x/ws')
    const connected = src.connect()
    currentWs().serverHello()
    await connected

    src.subscribe('asset', ['tok-1', 'tok-2'])
    expect(currentWs().lastSent()).toEqual({ op: 'sub', ch: 'asset', ids: ['tok-1', 'tok-2'] })

    src.subscribe('top')
    expect(currentWs().lastSent()).toEqual({ op: 'sub', ch: 'top' })

    // Drop the connection; LiveSource should schedule a reconnect.
    const dead = currentWs()
    dead.serverDrop()
    await vi.advanceTimersByTimeAsync(1000)
    expect(FakeWebSocket.instances.length).toBeGreaterThan(1)

    // New socket says hello -> every tracked subscription is re-sent.
    currentWs().serverHello()
    const sentOps = currentWs().sent.map((s) => JSON.parse(s))
    expect(sentOps).toContainEqual({ op: 'sub', ch: 'asset', ids: ['tok-1', 'tok-2'] })
    expect(sentOps).toContainEqual({ op: 'sub', ch: 'top' })
  })

  it('delivers ticks to registered handlers', async () => {
    const src = new LiveSource('ws://x/ws')
    const connected = src.connect()
    currentWs().serverHello()
    await connected

    const received: Tick[][] = []
    src.on('ticks', (ticks) => received.push(ticks))

    const tick: Tick = {
      v: 1,
      a: 'tok-1',
      m: 'mkt-1',
      p: 0.63,
      b: 0.62,
      k: 0.64,
      c1m: 1.2,
      c5m: -3.4,
      vol: 0.02,
      seq: 1,
      rts: 1000,
      pts: 1001,
    }
    currentWs().serverSend({ t: 'ticks', d: [tick] })

    expect(received).toEqual([[tick]])
  })

  it('reconnects with exponential backoff + jitter after repeated drops', async () => {
    // Jitter is delay * (0.5 + random*0.5); pin random so timing is
    // deterministic (jitter == exactly 50% of the nominal delay).
    vi.spyOn(Math, 'random').mockReturnValue(0)

    const src = new LiveSource('ws://x/ws')
    const connected = src.connect()
    currentWs().serverHello()
    await connected

    // First drop: nominal delay is BASE_DELAY_MS (500ms) -> jitter = 250ms.
    currentWs().serverDrop()
    await vi.advanceTimersByTimeAsync(249)
    expect(FakeWebSocket.instances).toHaveLength(1)
    await vi.advanceTimersByTimeAsync(2)
    expect(FakeWebSocket.instances).toHaveLength(2)

    // Second drop without ever completing a hello: nominal delay doubles to
    // 1000ms -> jitter = 500ms, strictly longer than the first wait.
    currentWs().serverDrop()
    await vi.advanceTimersByTimeAsync(499)
    expect(FakeWebSocket.instances).toHaveLength(2)
    await vi.advanceTimersByTimeAsync(2)
    expect(FakeWebSocket.instances).toHaveLength(3)
  })

  it('stops reconnecting once close() is called by the user', async () => {
    const src = new LiveSource('ws://x/ws')
    const connected = src.connect()
    currentWs().serverHello()
    await connected

    src.close()
    await vi.advanceTimersByTimeAsync(60_000)
    expect(FakeWebSocket.instances).toHaveLength(1)
  })
})

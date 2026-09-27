/// <reference types="node" />
import { gzipSync } from 'node:zlib'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { ReplaySource } from './replay-source'
import type { Alert, Tick } from './types'

function tick(a: string, pts: number, c5m = 0): Tick {
  return { v: 1, a, m: 'mkt-1', p: 0.5, b: 0.49, k: 0.51, c1m: 0, c5m, vol: 0, seq: 1, rts: pts, pts }
}

function alert(a: string, ts: number): Alert {
  return { v: 1, a, m: 'mkt-1', q: 'Will X happen?', outcome: 'Yes', from: 0.4, to: 0.6, window_s: 60, ts }
}

// Builds a fake fetch that serves a manifest + one gzip NDJSON file + a
// markets.json, mirroring what scripts/replay-export.sh publishes under
// web/public/replay/.
function fakeFetch(lines: string[]): typeof fetch {
  const ndjson = lines.join('\n') + '\n'
  const gz = gzipSync(Buffer.from(ndjson))
  const manifest = {
    recorded_at: '2026-09-27T08:00:00Z',
    duration_s: 2,
    files: ['clip.ndjson.gz'],
    markets: 'markets.json',
    benchmark: 'benchmark.json',
  }
  return vi.fn(async (url: string | URL | Request) => {
    const u = String(url)
    if (u.endsWith('manifest.json')) return new Response(JSON.stringify(manifest))
    if (u.endsWith('clip.ndjson.gz')) return new Response(gz)
    if (u.endsWith('markets.json')) return new Response(JSON.stringify([{ market_id: 'mkt-1' }]))
    throw new Error(`unexpected fetch ${u}`)
  }) as unknown as typeof fetch
}

beforeEach(() => {
  vi.useFakeTimers()
})

describe('ReplaySource', () => {
  it('loads the manifest, decompresses the clip, and replays ticks/alerts in order', async () => {
    // Original spacing: 600ms tick->alert, 900ms alert->tick2. originStart
    // is the first event's own ts (1000), so "elapsed" below is relative to
    // that, not to wall-clock start.
    const lines = [
      JSON.stringify({ t: 'tick', d: tick('a1', 1000, 2) }),
      JSON.stringify({ t: 'alert', d: alert('a1', 1600) }),
      JSON.stringify({ t: 'tick', d: tick('a1', 2500, -3) }),
    ]
    vi.stubGlobal('fetch', fakeFetch(lines))

    const src = new ReplaySource('/replay/')
    const ticks: Tick[][] = []
    const alerts: Alert[] = []
    const statuses: string[] = []
    src.on('ticks', (t) => ticks.push(t))
    src.on('alert', (a) => alerts.push(a))
    src.on('status', (s) => statuses.push(s))

    await src.connect()
    expect(statuses).toEqual(['connecting', 'open'])
    expect(src.manifest?.recorded_at).toBe('2026-09-27T08:00:00Z')

    // At 1x speed, elapsed==0 for the first event (its own ts is the
    // origin), so it fires on the very first 50ms pump tick.
    await vi.advanceTimersByTimeAsync(500)
    expect(ticks).toHaveLength(1)
    expect(ticks[0][0].a).toBe('a1')
    expect(alerts).toHaveLength(0)

    await vi.advanceTimersByTimeAsync(500) // elapsed now ~1000ms >= alert's 600ms offset
    expect(alerts).toHaveLength(1)

    await vi.advanceTimersByTimeAsync(500) // elapsed now 1500ms, exactly tick2's offset
    expect(ticks).toHaveLength(2)

    src.close()
  })

  it('loops back to the start once every event has replayed', async () => {
    const lines = [
      JSON.stringify({ t: 'tick', d: tick('a1', 0, 1) }),
      JSON.stringify({ t: 'tick', d: tick('a1', 1000, 2) }),
    ]
    vi.stubGlobal('fetch', fakeFetch(lines))

    const src = new ReplaySource('/replay/')
    const ticks: Tick[][] = []
    src.on('ticks', (t) => ticks.push(t))
    await src.connect()

    // Both events replay once within the first 1000ms, which also triggers
    // the loop-back reset (playIndex reaches the end).
    await vi.advanceTimersByTimeAsync(1000)
    expect(ticks.length).toBe(2)

    // A full second lap replays both events again.
    await vi.advanceTimersByTimeAsync(1000)
    expect(ticks.length).toBe(4)

    src.close()
  })

  it('loads bundled market metadata as the replay-mode substitute for GET /markets', async () => {
    vi.stubGlobal('fetch', fakeFetch([]))
    const src = new ReplaySource('/replay/')
    await src.connect()
    const markets = await src.loadMarkets()
    expect(markets).toEqual([{ market_id: 'mkt-1' }])
    src.close()
  })
})

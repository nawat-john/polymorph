<script lang="ts">
  import { onMount } from 'svelte'
  import { createChart, LineSeries, type IChartApi, type ISeriesApi, type UTCTimestamp } from 'lightweight-charts'
  import { ticks, source } from '../lib/store'
  import { pct, pp } from '../lib/format'
  import type { Market } from '../feed/types'

  export let market: Market
  export let onBack: () => void = () => {}

  let selectedIdx = 0
  $: asset = market.clob_token_ids[selectedIdx] as string | undefined
  $: outcomeName = market.outcomes?.[selectedIdx] ?? `Outcome ${selectedIdx + 1}`
  $: currentTick = asset ? $ticks.get(asset) : undefined

  // Bounded ring buffer of chart points per asset (design-plan.md section 7:
  // "chart data is stored in a bounded ring buffer"), capped at the same
  // 15-minute/1-point-per-second horizon the processor itself keeps
  // (design-plan.md section 4.2). The gateway only streams live ticks
  // forward from the moment of subscription - there is no historical-price
  // endpoint - so the chart starts from the subscribe-time snapshot and
  // grows as real ticks arrive.
  const RING_MAX = 900
  const buffers = new Map<string, { time: UTCTimestamp; value: number }[]>()

  let chartEl: HTMLDivElement
  let chart: IChartApi | undefined
  let series: ISeriesApi<'Line'> | undefined

  onMount(() => {
    chart = createChart(chartEl, {
      autoSize: true,
      layout: { background: { color: 'transparent' }, textColor: '#7c8797' },
      grid: {
        vertLines: { color: '#232b3a' },
        horzLines: { color: '#232b3a' },
      },
      rightPriceScale: { borderColor: '#232b3a' },
      timeScale: { borderColor: '#232b3a', timeVisible: true },
    })
    series = chart.addSeries(LineSeries, { color: '#4da3ff', lineWidth: 2 })
    return () => chart?.remove()
  })

  onMount(() => {
    source.subscribe('asset', market.clob_token_ids)
    return () => source.unsubscribe('asset', market.clob_token_ids)
  })

  $: if (series && asset) {
    series.setData(buffers.get(asset) ?? [])
  }

  $: if (series && asset && currentTick) {
    const point = { time: Math.floor(currentTick.pts / 1000) as UTCTimestamp, value: currentTick.p }
    const buf = buffers.get(asset) ?? []
    if (buf.length > 0 && buf[buf.length - 1].time === point.time) {
      buf[buf.length - 1] = point
    } else {
      buf.push(point)
      if (buf.length > RING_MAX) buf.shift()
    }
    buffers.set(asset, buf)
    series.update(point)
  }
</script>

<div class="detail panel">
  <button class="back" on:click={onBack}>&larr; Back to Market Wall</button>
  <h2>{market.question}</h2>
  <p class="meta mono">
    {market.slug}
    {#if market.volume}· vol {market.volume.toLocaleString()}{/if}
  </p>

  <div class="outcomes">
    {#each market.clob_token_ids as id, i (id)}
      <button class="outcome" class:active={i === selectedIdx} on:click={() => (selectedIdx = i)}>
        {market.outcomes?.[i] ?? `Outcome ${i + 1}`}
        {#if $ticks.get(id)}<span class="mono">&nbsp;{pct($ticks.get(id)!.p)}</span>{/if}
      </button>
    {/each}
  </div>

  <div class="stats">
    <div><span class="label">Outcome</span><span>{outcomeName}</span></div>
    <div><span class="label">Price</span><span class="mono">{currentTick ? pct(currentTick.p) : '—'}</span></div>
    <div><span class="label">Bid</span><span class="mono">{currentTick ? pct(currentTick.b) : '—'}</span></div>
    <div><span class="label">Ask</span><span class="mono">{currentTick ? pct(currentTick.k) : '—'}</span></div>
    <div>
      <span class="label">Chg 1m</span>
      <span class="mono" class:up={(currentTick?.c1m ?? 0) > 0} class:down={(currentTick?.c1m ?? 0) < 0}
        >{currentTick ? pp(currentTick.c1m) : '—'}</span
      >
    </div>
    <div>
      <span class="label">Chg 5m</span>
      <span class="mono" class:up={(currentTick?.c5m ?? 0) > 0} class:down={(currentTick?.c5m ?? 0) < 0}
        >{currentTick ? pp(currentTick.c5m) : '—'}</span
      >
    </div>
  </div>

  <div class="chart" bind:this={chartEl}></div>
</div>

<style>
  .back {
    background: none;
    border: none;
    color: var(--accent);
    padding: 0;
    margin-bottom: 0.75rem;
    font-size: 0.9rem;
  }

  h2 {
    margin: 0 0 0.25rem;
  }

  .meta {
    color: var(--text-dim);
    margin: 0 0 1rem;
    font-size: 0.85rem;
  }

  .outcomes {
    display: flex;
    gap: 0.5rem;
    margin-bottom: 1rem;
    flex-wrap: wrap;
  }

  .outcome {
    background: var(--bg-tile);
    border: 1px solid var(--border);
    color: var(--text);
    border-radius: 6px;
    padding: 0.4rem 0.8rem;
  }

  .outcome.active {
    border-color: var(--accent);
    color: var(--accent);
  }

  .stats {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(110px, 1fr));
    gap: 0.75rem;
    margin-bottom: 1rem;
  }

  .stats > div {
    display: flex;
    flex-direction: column;
    gap: 0.15rem;
  }

  .label {
    color: var(--text-dim);
    font-size: 0.75rem;
  }

  .chart {
    height: 320px;
    width: 100%;
  }
</style>

<script lang="ts">
  import { derived } from 'svelte/store'
  import { markets, ticks, flashed } from '../lib/store'
  import { pct, pp, intensity, truncate } from '../lib/format'
  import type { Market } from '../feed/types'

  export let onSelect: (m: Market) => void = () => {}

  // "Top ~100 markets" (design-plan.md section 7), ranked by volume; one
  // tile per market using its first outcome (the asset actually ticking)
  // for price/color.
  const topMarkets = derived(markets, ($markets) =>
    [...$markets]
      .filter((m) => m.clob_token_ids.length > 0)
      .sort((a, b) => b.volume - a.volume)
      .slice(0, 100),
  )

  // asset id -> flash-until timestamp (ms), so only tiles that actually
  // changed this rAF batch flash, without recreating any tile's DOM node.
  let flashUntil = new Map<string, number>()
  const FLASH_MS = 350

  $: {
    if ($flashed.length > 0) {
      const until = performance.now() + FLASH_MS
      const m = new Map(flashUntil)
      for (const id of $flashed) m.set(id, until)
      flashUntil = m
      setTimeout(() => {
        flashUntil = new Map(flashUntil)
      }, FLASH_MS + 20)
    }
  }

  function isFlashing(assetID: string): boolean {
    const until = flashUntil.get(assetID)
    return until !== undefined && until > performance.now()
  }
</script>

<div class="wall">
  {#each $topMarkets as m (m.market_id)}
    {@const asset = m.clob_token_ids[0]}
    {@const tick = $ticks.get(asset)}
    {@const chg = tick?.c5m ?? 0}
    <button
      class="tile"
      class:up={chg > 0}
      class:down={chg < 0}
      class:flash={isFlashing(asset)}
      style="--i: {intensity(chg)}"
      on:click={() => onSelect(m)}
      title={m.question}
    >
      <div class="q">{truncate(m.question, 60)}</div>
      <div class="row">
        <span class="price mono">{tick ? pct(tick.p) : '—'}</span>
        <span class="chg mono">{tick ? pp(tick.c5m) : ''}</span>
      </div>
    </button>
  {/each}
  {#if $topMarkets.length === 0}
    <p class="empty">Waiting for market metadata from the gateway…</p>
  {/if}
</div>

<style>
  .wall {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(180px, 1fr));
    gap: 0.5rem;
  }

  .tile {
    display: block;
    text-align: left;
    background: var(--bg-tile);
    border: 1px solid var(--border);
    border-radius: 6px;
    padding: 0.6rem 0.7rem;
    color: var(--text);
    transition:
      background-color 150ms ease,
      transform 150ms ease;
  }

  .tile.up {
    background-color: color-mix(in srgb, var(--green) calc(var(--i) * 45%), var(--bg-tile));
    border-color: var(--green-strong);
  }

  .tile.down {
    background-color: color-mix(in srgb, var(--red) calc(var(--i) * 45%), var(--bg-tile));
    border-color: var(--red-strong);
  }

  .tile.flash {
    transform: scale(1.03);
  }

  .q {
    font-size: 0.8rem;
    color: var(--text-dim);
    margin-bottom: 0.4rem;
    min-height: 2.2em;
  }

  .row {
    display: flex;
    justify-content: space-between;
    font-size: 0.95rem;
    font-weight: 600;
  }

  .empty {
    color: var(--text-dim);
    grid-column: 1 / -1;
  }
</style>

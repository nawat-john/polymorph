<script lang="ts">
  import { derived } from 'svelte/store'
  import { top, assetByID, ticks } from '../lib/store'
  import { pct, pp, truncate } from '../lib/format'

  const ranked = derived(top, ($top) => [...$top].sort((a, b) => Math.abs(b.c5m) - Math.abs(a.c5m)))
</script>

<div class="movers panel">
  <h3>Top Movers</h3>
  {#if $ranked.length === 0}
    <p class="empty">Waiting for the "top" channel (updated ~1/s by the processor).</p>
  {:else}
    <table>
      <thead>
        <tr>
          <th>Market</th>
          <th>Price</th>
          <th>Chg 5m</th>
        </tr>
      </thead>
      <tbody>
        {#each $ranked as row (row.a)}
          {@const info = $assetByID.get(row.a)}
          {@const tick = $ticks.get(row.a)}
          <tr>
            <td>{info ? truncate(info.market.question, 50) : row.a}</td>
            <td class="mono">{tick ? pct(tick.p) : '—'}</td>
            <td class="mono" class:up={row.c5m > 0} class:down={row.c5m < 0}>{pp(row.c5m)}</td>
          </tr>
        {/each}
      </tbody>
    </table>
  {/if}
</div>

<style>
  h3 {
    margin: 0 0 0.75rem;
  }

  th,
  td {
    text-align: left;
    padding: 0.35rem 0.5rem;
    font-size: 0.85rem;
    border-bottom: 1px solid var(--border);
  }

  th {
    color: var(--text-dim);
    font-weight: 500;
  }

  .empty {
    color: var(--text-dim);
    font-size: 0.85rem;
  }
</style>

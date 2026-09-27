<script lang="ts">
  import { alerts, assetByID } from '../lib/store'
  import { pct } from '../lib/format'

  function fmtTime(ts: number): string {
    return new Date(ts).toLocaleTimeString()
  }
</script>

<div class="feed panel">
  <h3>Surge Feed</h3>
  {#if $alerts.length === 0}
    <p class="empty">No surges yet. Alerts appear here when a price moves fast (design-plan.md §4.2's surge detector).</p>
  {/if}
  <ul>
    {#each $alerts as a (a.a + a.ts)}
      <li>
        <span class="time mono">{fmtTime(a.ts)}</span>
        <span class="body">
          <strong>{a.outcome}</strong>: {pct(a.from)} → {pct(a.to)} in {a.window_s}s
          <span class="q">— {$assetByID.get(a.a)?.market.question ?? a.q}</span>
        </span>
      </li>
    {/each}
  </ul>
</div>

<style>
  h3 {
    margin: 0 0 0.75rem;
  }

  ul {
    list-style: none;
    margin: 0;
    padding: 0;
    display: flex;
    flex-direction: column;
    gap: 0.5rem;
    max-height: 320px;
    overflow-y: auto;
  }

  li {
    display: flex;
    gap: 0.6rem;
    font-size: 0.85rem;
    border-bottom: 1px solid var(--border);
    padding-bottom: 0.4rem;
  }

  .time {
    color: var(--text-dim);
    white-space: nowrap;
  }

  .q {
    color: var(--text-dim);
  }

  .empty {
    color: var(--text-dim);
    font-size: 0.85rem;
  }
</style>

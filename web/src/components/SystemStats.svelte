<script lang="ts">
  import { sys, status } from '../lib/store'
</script>

<div class="stats panel">
  <div class="badges">
    <span class="mode">LIVE</span>
    <span class="conn" class:open={$status === 'open'} class:closed={$status !== 'open'}>
      {$status === 'open' ? 'connected' : $status}
    </span>
  </div>
  <div class="grid">
    <div><span class="label">Clients online</span><span class="mono">{$sys?.clients ?? '—'}</span></div>
    <div><span class="label">In events/s</span><span class="mono">{$sys ? $sys.in_eps.toFixed(0) : '—'}</span></div>
    <div><span class="label">Out msgs/s</span><span class="mono">{$sys ? $sys.out_mps.toFixed(0) : '—'}</span></div>
    <div><span class="label">p99 latency</span><span class="mono">{$sys ? `${$sys.p99_ms.toFixed(1)} ms` : '—'}</span></div>
  </div>
</div>

<style>
  .badges {
    display: flex;
    gap: 0.5rem;
    margin-bottom: 0.75rem;
  }

  .mode,
  .conn {
    font-size: 0.7rem;
    font-weight: 700;
    letter-spacing: 0.05em;
    border-radius: 4px;
    padding: 0.15rem 0.5rem;
  }

  .mode {
    background: var(--green-strong);
    color: #04140a;
  }

  .conn.open {
    background: var(--bg-tile);
    color: var(--green);
    border: 1px solid var(--green-strong);
  }

  .conn.closed {
    background: var(--bg-tile);
    color: var(--red);
    border: 1px solid var(--red-strong);
  }

  .grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(110px, 1fr));
    gap: 0.6rem;
  }

  .grid > div {
    display: flex;
    flex-direction: column;
    gap: 0.15rem;
  }

  .label {
    color: var(--text-dim);
    font-size: 0.7rem;
  }
</style>

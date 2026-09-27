<script lang="ts">
  import { sys, status, mode, replayRecordedAt, tryLive, setReplaySpeed } from '../lib/store'

  let trying = false
  let speed = 1
  const SPEEDS = [1, 5, 20]

  function onSpeedChange(): void {
    setReplaySpeed(speed)
  }

  function fmtRecordedAt(iso: string | null): string {
    if (!iso) return 'recorded clip'
    return new Date(iso).toLocaleString()
  }

  async function onTryLive(): Promise<void> {
    trying = true
    await tryLive()
    trying = false
  }
</script>

<div class="stats panel">
  <div class="badges">
    {#if $mode === 'replay'}
      <span class="mode replay">REPLAY — recorded {fmtRecordedAt($replayRecordedAt)}</span>
      <select class="speed" bind:value={speed} on:change={onSpeedChange} aria-label="Replay speed">
        {#each SPEEDS as s (s)}
          <option value={s}>{s}x</option>
        {/each}
      </select>
      <button class="try-live" on:click={onTryLive} disabled={trying}>{trying ? 'Trying…' : 'Try live'}</button>
    {:else}
      <span class="mode">LIVE</span>
      <span class="conn" class:open={$status === 'open'} class:closed={$status !== 'open'}>
        {$status === 'open' ? 'connected' : $status}
      </span>
    {/if}
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

  .mode.replay {
    background: var(--bg-tile);
    color: var(--accent);
    border: 1px solid var(--accent);
  }

  .try-live {
    font-size: 0.7rem;
    font-weight: 700;
    border-radius: 4px;
    padding: 0.15rem 0.5rem;
    background: none;
    border: 1px solid var(--accent);
    color: var(--accent);
  }

  .try-live:disabled {
    opacity: 0.6;
  }

  .speed {
    font-size: 0.7rem;
    background: var(--bg-tile);
    color: var(--text);
    border: 1px solid var(--border);
    border-radius: 4px;
    padding: 0.1rem 0.3rem;
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

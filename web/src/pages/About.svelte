<script lang="ts">
  const GITHUB_URL = 'https://github.com/nawat-john/oddspulse'
</script>

<div class="about panel">
  <h2>About / Architecture</h2>
  <p>
    OddsPulse tracks live Polymarket prediction-market prices through a Kafka (Redpanda) pipeline and fans them out
    to many browsers over WebSocket. See <code>docs/design-plan.md</code> in the repo for the full design.
  </p>

  <svg class="diagram" viewBox="0 0 720 160" role="img" aria-label="Architecture: Polymarket to ingestor to Redpanda to processor to gateway to browser">
    <defs>
      <marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
        <path d="M0 0L10 5L0 10z" fill="var(--text-dim)" />
      </marker>
    </defs>
    {#each [
      { x: 10, label: 'Polymarket\nGamma + WS' },
      { x: 150, label: 'ingestor' },
      { x: 290, label: 'Redpanda\n(pm.raw/ticks/...)' },
      { x: 430, label: 'processor' },
      { x: 570, label: 'gateway(s)' },
    ] as box, i}
      <rect x={box.x} y="60" width="120" height="50" rx="6" fill="var(--bg-tile)" stroke="var(--border)" />
      <text x={box.x + 60} y="80" text-anchor="middle" fill="var(--text)" font-size="11">
        {#each box.label.split('\n') as line, j}
          <tspan x={box.x + 60} dy={j === 0 ? 0 : 12}>{line}</tspan>
        {/each}
      </text>
      {#if i < 4}
        <line x1={box.x + 120} y1="85" x2={box.x + 140} y2="85" stroke="var(--text-dim)" marker-end="url(#arrow)" />
      {/if}
    {/each}
    <rect x="570" y="10" width="120" height="30" rx="6" fill="var(--bg-tile)" stroke="var(--accent)" />
    <text x="630" y="30" text-anchor="middle" fill="var(--accent)" font-size="11">Browser (this page)</text>
    <line x1="630" y1="40" x2="630" y2="58" stroke="var(--accent)" marker-end="url(#arrow)" />
  </svg>

  <h3>Data flow</h3>
  <ol>
    <li>The ingestor connects to Polymarket's Gamma API and CLOB WebSocket and produces raw events to <code>pm.raw</code>.</li>
    <li>The processor computes change/volatility and surge alerts, producing <code>pm.ticks</code>, <code>pm.alerts</code> and <code>pm.top</code>.</li>
    <li>Each gateway instance broadcasts those topics to its connected WebSocket clients, batched every 100ms.</li>
    <li>This page connects over <code>wss://</code> (or <code>ws://</code> locally) and renders the Market Wall, Surge Feed, Top Movers and System Stats panels from that stream.</li>
  </ol>

  <h3>Links</h3>
  <ul>
    <li><a href={GITHUB_URL} target="_blank" rel="noreferrer">Source on GitHub</a></li>
    <li>Benchmark results: not yet published (Phase 6 of the project plan) - <code>web/public/replay/benchmark.json</code> will carry them once measured.</li>
  </ul>
</div>

<style>
  .diagram {
    width: 100%;
    height: auto;
    margin: 1rem 0;
  }

  h3 {
    margin-top: 1.25rem;
  }

  code {
    background: var(--bg-tile);
    padding: 0.1rem 0.3rem;
    border-radius: 4px;
  }
</style>

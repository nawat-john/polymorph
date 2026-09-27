<script lang="ts">
  import { onMount } from 'svelte'
  import MarketWall from './components/MarketWall.svelte'
  import MarketDetail from './components/MarketDetail.svelte'
  import SurgeFeed from './components/SurgeFeed.svelte'
  import TopMovers from './components/TopMovers.svelte'
  import SystemStats from './components/SystemStats.svelte'
  import About from './pages/About.svelte'
  import { start } from './lib/store'
  import type { Market } from './feed/types'

  type Tab = 'wall' | 'movers' | 'about'
  let tab: Tab = 'wall'
  let selectedMarket: Market | null = null

  onMount(() => {
    start()
  })

  function select(m: Market): void {
    selectedMarket = m
  }
  function back(): void {
    selectedMarket = null
  }
  function goto(t: Tab): void {
    tab = t
    selectedMarket = null
  }
</script>

<header>
  <h1>OddsPulse</h1>
  <nav>
    <button class:active={tab === 'wall'} on:click={() => goto('wall')}>Market Wall</button>
    <button class:active={tab === 'movers'} on:click={() => goto('movers')}>Movers &amp; Surges</button>
    <button class:active={tab === 'about'} on:click={() => goto('about')}>About</button>
  </nav>
</header>

<main>
  {#if tab === 'wall'}
    {#if selectedMarket}
      <MarketDetail market={selectedMarket} onBack={back} />
    {:else}
      <MarketWall onSelect={select} />
    {/if}
  {:else if tab === 'movers'}
    <div class="split">
      <TopMovers />
      <SurgeFeed />
    </div>
  {:else}
    <About />
  {/if}
</main>

<aside>
  <SystemStats />
</aside>

<style>
  header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 0.75rem 1rem;
    border-bottom: 1px solid var(--border);
    flex-wrap: wrap;
    gap: 0.5rem;
  }

  h1 {
    font-size: 1.1rem;
    margin: 0;
    letter-spacing: 0.02em;
  }

  nav {
    display: flex;
    gap: 0.4rem;
  }

  nav button {
    background: none;
    border: 1px solid var(--border);
    color: var(--text-dim);
    border-radius: 6px;
    padding: 0.35rem 0.7rem;
    font-size: 0.85rem;
  }

  nav button.active {
    color: var(--text);
    border-color: var(--accent);
  }

  main {
    padding: 1rem;
    flex: 1;
  }

  aside {
    padding: 0 1rem 1rem;
  }

  .split {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 1rem;
  }

  @media (max-width: 720px) {
    .split {
      grid-template-columns: 1fr;
    }

    header {
      flex-direction: column;
      align-items: flex-start;
    }
  }
</style>

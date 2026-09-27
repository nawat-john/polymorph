import { svelte } from '@sveltejs/vite-plugin-svelte'
import { defineConfig } from 'vitest/config'

// https://vite.dev/config/
export default defineConfig({
  plugins: [svelte()],
  // GitHub Pages serves the site at https://<user>.github.io/oddspulse/,
  // so every asset URL must be prefixed (design-plan.md section 7/13).
  // Placeholder repo name until the real GitHub repo is created.
  base: '/oddspulse/',
  test: {
    environment: 'node',
  },
})

import { svelte } from '@sveltejs/vite-plugin-svelte'
import { defineConfig } from 'vitest/config'

// https://vite.dev/config/
export default defineConfig({
  plugins: [svelte()],
  // GitHub Pages serves a project site under /<repo>/ (here nawat-john/
  // polymorph), so every asset URL must be prefixed (design-plan.md
  // section 7/13). Change this if the repo is renamed.
  base: '/polymorph/',
  test: {
    environment: 'node',
  },
})

import { API_BASE } from './config'
import type { Market } from '../feed/types'

/** Fetches the gateway's bootstrap-loaded market metadata (GET /markets). */
export async function fetchMarkets(): Promise<Market[]> {
  const res = await fetch(`${API_BASE}/markets`)
  if (!res.ok) throw new Error(`GET /markets: ${res.status}`)
  return res.json()
}

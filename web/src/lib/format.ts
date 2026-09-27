/** Formats a probability (0..1) as a percentage, e.g. 0.634 -> "63.4%". */
export function pct(p: number): string {
  return `${(p * 100).toFixed(1)}%`
}

/** Formats a percentage-point change with an explicit sign, e.g. 3.4 -> "+3.4pp". */
export function pp(x: number): string {
  const sign = x > 0 ? '+' : ''
  return `${sign}${x.toFixed(1)}pp`
}

/** Clamps a pp change into 0..1 for use as a CSS color-intensity input. */
export function intensity(chg: number, max = 10): number {
  return Math.min(1, Math.abs(chg) / max)
}

export function truncate(s: string, n: number): string {
  return s.length > n ? `${s.slice(0, n - 1)}…` : s
}

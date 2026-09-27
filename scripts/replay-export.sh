#!/bin/bash
# Publishes Replay Mode's data (design-plan.md sections 4.4/8): scans every
# recorded data/replay/YYYY-MM-DD/HH.ndjson.gz file, picks the one with the
# most alerts/surges ("interesting"), trims it to a duration and size
# budget, and writes it plus manifest.json/markets.json/benchmark.json to
# web/public/replay/ for the frontend to serve statically.
#
# With only a short real recording (cmd/recorder run for a few minutes
# against live Polymarket data, not the 30-60 minutes this normally trims
# to), there is only ever one real candidate file - the selection logic
# below still runs for real, it just has little to choose from yet. Re-run
# this script after a longer/more eventful recording (design-plan.md
# Phase 7: "record a new replay during a big event") to get a richer clip.
set -euo pipefail

SRC_DIR="${REPLAY_SRC_DIR:-data/replay}"
OUT_DIR="${REPLAY_OUT_DIR:-web/public/replay}"
MAX_BYTES="${REPLAY_MAX_BYTES:-5242880}"        # 5MB (design-plan.md 4.4)
MAX_DURATION_S="${REPLAY_MAX_DURATION_S:-3600}" # upper bound; 4.4/8 say "normally 30-60 min"
MARKETS_URL="${REPLAY_MARKETS_URL:-http://localhost:8081/markets}"
MARKETS_FILE="${REPLAY_MARKETS_FILE:-}"

command -v node >/dev/null || { echo "node is required (JSON parsing)" >&2; exit 1; }

mapfile -t FILES < <(find "$SRC_DIR" -name '*.ndjson.gz' -type f | sort)
if [ "${#FILES[@]}" -eq 0 ]; then
  echo "no recorded files under $SRC_DIR - run cmd/recorder first" >&2
  exit 1
fi

echo "scanning ${#FILES[@]} recorded file(s) for alert density..."
best_file=""
best_alerts=-1
for f in "${FILES[@]}"; do
  n=$(zcat "$f" 2>/dev/null | grep -c '"t":"alert"' || true)
  echo "  $f: $n alerts"
  if [ "$n" -gt "$best_alerts" ]; then
    best_alerts="$n"
    best_file="$f"
  fi
done
echo "selected $best_file ($best_alerts alerts)"

mkdir -p "$OUT_DIR"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

zcat "$best_file" > "$work/full.ndjson"

# 1. Duration trim: keep only the tail within MAX_DURATION_S of the last
# event's own timestamp (Tick.pts / Alert.ts), not wall-clock file time.
node -e '
const fs = require("fs")
const [inFile, outFile, maxDurS] = process.argv.slice(1)
const lines = fs.readFileSync(inFile, "utf8").split("\n").filter(Boolean)
const ts = (line) => { const r = JSON.parse(line); return r.t === "tick" ? r.d.pts : r.d.ts }
const lastTs = ts(lines[lines.length - 1])
const cutoff = lastTs - Number(maxDurS) * 1000
const kept = lines.filter((l) => ts(l) >= cutoff)
fs.writeFileSync(outFile, kept.join("\n") + "\n")
' "$work/full.ndjson" "$work/duration.ndjson" "$MAX_DURATION_S"

# 2. Size trim: drop oldest lines until the *compressed* output fits
# MAX_BYTES, re-checking each time since the gzip ratio depends on which
# lines remain.
# ponytail: linear 10%-shrink search, not a binary search - the line counts
# involved here are in the tens of thousands at most (an hour of ticks),
# so this is a handful of iterations; revisit if a much larger source file
# ever needs trimming.
total_lines=$(wc -l < "$work/duration.ndjson")
keep_lines=$total_lines
while :; do
  tail -n "$keep_lines" "$work/duration.ndjson" | gzip -9 > "$work/candidate.gz"
  size=$(wc -c < "$work/candidate.gz")
  if [ "$size" -le "$MAX_BYTES" ] || [ "$keep_lines" -le 1 ]; then
    break
  fi
  keep_lines=$(( keep_lines * 9 / 10 ))
  [ "$keep_lines" -ge 1 ] || keep_lines=1
done
tail -n "$keep_lines" "$work/duration.ndjson" > "$work/final.ndjson"
final_size=$(wc -c < "$work/candidate.gz")
echo "kept $keep_lines/$total_lines lines, ${final_size} bytes gzipped"

# recorded_at / duration_s come from the kept events' own timestamps, not
# wall-clock export time.
read -r recorded_at duration_s <<< "$(node -e '
const fs = require("fs")
const lines = fs.readFileSync(process.argv[1], "utf8").split("\n").filter(Boolean)
const ts = (line) => { const r = JSON.parse(line); return r.t === "tick" ? r.d.pts : r.d.ts }
const first = ts(lines[0])
const last = ts(lines[lines.length - 1])
console.log(new Date(first).toISOString(), Math.round((last - first) / 1000))
' "$work/final.ndjson")"

date_part=$(basename "$(dirname "$best_file")")
hour_part=$(basename "$best_file" .ndjson.gz)
out_name="replay-${date_part}-${hour_part}.ndjson.gz"
cp "$work/candidate.gz" "$OUT_DIR/$out_name"

# markets.json: real market metadata (internal/model/market.go schema), so
# the frontend can show real questions/outcomes in replay mode without a
# live GET /markets. Prefer an already-fetched file (REPLAY_MARKETS_FILE),
# else fetch the live gateway, else leave whatever is already in OUT_DIR.
if [ -n "$MARKETS_FILE" ]; then
  cp "$MARKETS_FILE" "$OUT_DIR/markets.json"
  echo "markets.json: copied from $MARKETS_FILE"
elif curl -fsS "$MARKETS_URL" -o "$work/markets.json" 2>/dev/null; then
  cp "$work/markets.json" "$OUT_DIR/markets.json"
  echo "markets.json: fetched from $MARKETS_URL"
elif [ -f "$OUT_DIR/markets.json" ]; then
  echo "markets.json: left existing file unchanged ($MARKETS_URL unreachable)"
else
  echo "markets.json: WARNING - none available (no $MARKETS_FILE, $MARKETS_URL unreachable, no existing file)" >&2
fi

# benchmark.json: Phase 6 (Load Test & Tuning) has not run yet - an honest
# placeholder, never fabricated numbers (design-plan.md section 9).
if [ ! -f "$OUT_DIR/benchmark.json" ]; then
  cat > "$OUT_DIR/benchmark.json" <<'EOF'
{
  "note": "No load test has been run yet - see design-plan.md Phase 6 (Load Test & Tuning). This file is a placeholder; System Stats shows live/replay numbers instead of anything from here until it is populated with real results."
}
EOF
fi

cat > "$OUT_DIR/manifest.json" <<EOF
{
  "recorded_at": "$recorded_at",
  "duration_s": $duration_s,
  "files": ["$out_name"],
  "markets": "markets.json",
  "benchmark": "benchmark.json"
}
EOF

echo "wrote $OUT_DIR/manifest.json (recorded_at=$recorded_at duration_s=$duration_s)"

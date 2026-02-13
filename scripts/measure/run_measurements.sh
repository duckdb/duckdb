#!/usr/bin/env bash
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULTS="$SCRIPT_DIR/results.txt"

: > "$RESULTS"

SQL_FILES=(
  0_cold
  1_cold_interleaved
  1_cold_segmented
  5_cold_interleaved
  5_cold_segmented
  10_cold_interleaved
  10_cold_segmented
  100_cold_interleaved
  100_cold_segmented
)

for base in "${SQL_FILES[@]}"; do
  echo "running ${base}"
  build/release/duckdb -f "$SCRIPT_DIR/${base}.sql"
done

for base in "${SQL_FILES[@]}"; do
  json="$SCRIPT_DIR/${base}.json"
  [ -f "$json" ] || continue
  cpu_time=$(jq -r '.cpu_time' "$json")
  probe_time=$(jq -r '[.. | objects | .extra_info["Probe Time"]? // empty] | first // "N/A"' "$json")
  {
    echo "$base"
    echo "CPU Time"
    echo "$cpu_time"
    echo "Probe Time"
    echo "$probe_time"
    echo
  } >> "$RESULTS"
done
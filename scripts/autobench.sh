set -e  # exit on first error

echo ""
echo "=============================================================="
echo "🦆📊 DuckDB AutoBench — Automated Benchmark Runner & Comparator"
echo "=============================================================="
echo ""
echo "\033[1;34mStarting automated build and benchmark comparison...\033[0m"
echo ""

# Load environment variables if running from a shell that didn’t source .zshrc
if [ -f "$HOME/.zshrc" ]; then
    source "$HOME/.zshrc"
fi
# --- Check required environment variables ---
if [ -z "$NEW_PATH" ]; then
  echo "\033[1;31m❌ ERROR:\033[0m NEW_PATH is not set. Example usage:"
  echo "NEW_PATH=/absolute/path/to/duckdb ./autobench.sh"
  exit 1
fi

if [ -z "$OLD_PATH" ]; then
  echo "\033[1;31m❌ ERROR:\033[0m OLD_PATH is not set. Example usage:"
  echo "OLD_PATH=/absolute/path/to/duckdb ./autobench.sh"
  exit 1
fi

if [ -z "$BENCHMARK_PATTERN" ]; then
  echo "\033[1;31m❌ ERROR:\033[0m BENCHMARK_PATTERN is not set. Example usage:"
  echo "BENCHMARK_PATTERN=benchmark/micro/compression/dictionary/.* ./autobench.sh"
  exit 1
fi

echo "📁 Using NEW_PATH: \033[1;33m$NEW_PATH\033[0m"
echo "📁 Using OLD_PATH: \033[1;33m$OLD_PATH\033[0m"
echo ""

# --- Ensure plots/timings directory exists ---
mkdir -p "$NEW_PATH/plots/timings"

run_build_and_benchmarks() {
  local PATH_DIR=$1
  local TAG=$2
  local TIMINGS_FILE="$NEW_PATH/plots/timings/timings_${TAG}.out"

  echo "\033[1;36mBuilding $TAG...\033[0m"
  BUILD_OUTPUT=$(mktemp)
  trap 'rm -f "$BUILD_OUTPUT"' EXIT

  # Capture both stdout and stderr, and the exit code
  set +e  # Temporarily disable exit on error
  BUILD_BENCHMARK=1 CORE_EXTENSIONS='tpch' make -C "$PATH_DIR" 2>&1 | tee "$BUILD_OUTPUT"
  BUILD_EXIT_CODE=${PIPESTATUS[0]}  # Get exit code from make, not tee
  set -e  # Re-enable exit on error

  if [ $BUILD_EXIT_CODE -ne 0 ]; then
    echo"\033[1;31m❌ Build failed for $TAG with exit code $BUILD_EXIT_CODE\033[0m"
    rm -f "$BUILD_OUTPUT"
    exit 1
  fi

  # Detect whether Ninja built anything
  if grep -qE 'Building CXX object|Linking CXX executable' "$BUILD_OUTPUT"; then
    BUILT=true
  else
    BUILT=false
  fi
  rm -f "$BUILD_OUTPUT"

  # Check if timing file exists
  if [ ! -f "$TIMINGS_FILE" ]; then
    TIMINGS_MISSING=true
  else
    TIMINGS_MISSING=false
  fi

  if [ "$BUILT" = true ] || [ "$TIMINGS_MISSING" = true ]; then
    echo "\033[1;32mRunning benchmarks for $TAG...\033[0m"
    "$PATH_DIR/build/release/benchmark/benchmark_runner" "$BENCHMARK_PATTERN" --root-dir "$NEW_PATH" \
      2>&1 | tee "$TIMINGS_FILE"
  else
    echo "\033[1;33mSkipping benchmarks for $TAG (no code changes and timings file exists).\033[0m"
  fi
}

# --- Build and benchmark both versions ---
run_build_and_benchmarks "$NEW_PATH" "new"
run_build_and_benchmarks "$OLD_PATH" "old"

# --- Plot comparison ---
echo "\033[1;35mPlotting comparison...\033[0m"
python3 "$NEW_PATH"/scripts/secondary_scripts/autobench.py --new_path $NEW_PATH
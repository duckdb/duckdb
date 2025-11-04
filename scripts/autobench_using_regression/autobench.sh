set -e  # exit on first error

echo ""
echo "=============================================================="
echo "🦆  DuckDB AutoBench — Automated Benchmark Runner & Comparator"
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

run_build() {
  local PATH_DIR=$1

  echo -e "\033[1;36mBuilding $PATH_DIR...\033[0m"

  # Capture both stdout and stderr, and the exit code
  set +e  # Temporarily disable exit on error
  BUILD_BENCHMARK=1 CORE_EXTENSIONS='tpch' GEN=ninja make -C "$PATH_DIR"
  BUILD_EXIT_CODE=$?
  set -e  # Re-enable exit on error

  if [ $BUILD_EXIT_CODE -ne 0 ]; then
    echo -e "\033[1;31m❌ Build failed for $PATH_DIR with exit code $BUILD_EXIT_CODE\033[0m"
    exit 1
  fi
}

# --- Build and benchmark both versions ---
run_build "$NEW_PATH"
run_build "$OLD_PATH"

# --- Run benchmarks and plot comparison ---
python3 autobench.py --old_path $OLD_PATH --new_path $NEW_PATH --benchmark_pattern $BENCHMARK_PATTERN
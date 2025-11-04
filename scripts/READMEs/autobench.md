# 🦆📊 DuckDB AutoBench

`AutoBench` is a utility script designed to automate the process of comparing micro-benchmarks between two different DuckDB repository checkouts (e.g., a fork feature branch vs. the `main` branch).

It handles building both versions, running the given set of benchmarks, and generating a single plot that compares the median runtimes with percentage changes.
## Usage 
First, install the requirements: ``  pip install pandas plotly kaleido``
### Example
```bash
python3 scripts/autobench.py --old_path "/path/to/my/duckdb_main"  --new_path "/path/to/my/duckdb_fork" --benchmark "benchmark/parquet/.*"
  ```

You must provide three environment variables to run the script.
Make sure you have cloned duckdb twice, one should be the main branch, and the other is your forked version, for development.

* `old_path`: The absolute path to the "old" (e.g., `main`) branch repository.
* `new_path`: The absolute path to your "new" or "feature" branch forked repository.
* `benchmark_pattern`: The RELATIVE path to a .benchmark file (e.g. benchmark/parquet/parquet_load.benchmark), or a regex string to select which benchmarks to run,  (e.g., `benchmark/parquet/.*` will run all inside the parquet folder).

#### Tip:
Set an alias with your new and old path, so in the future you can just invoke `autobench` and only need to pass the benchmark you want
```bash
echo "alias autobench='python3 scripts/autobench.py --old_path \"/path/to/my/duckdb_main\" --new_path \"/path/to/my/duckdb_fork\"'" >> ~/.zshrc && source ~/.zshrc
```
## What It Does: The Workflow

The script performs the following steps:

1.  **Setup:** Creates an output directory at `$NEW_PATH/plots/timings/` if it doesn't exist.
2.  **Build & Run (Both Versions):**
    * It calls the `run_build_and_benchmarks` function for both `OLD_PATH` (tagged "old") and `NEW_PATH` (tagged "new").
    * For each version, it builds the benchmark runner (`BUILD_BENCHMARK=1 CORE_EXTENSIONS='tpch' make ...`).
    * It runs the specified benchmarks using the built runner (`build/release/benchmark/benchmark_runner "$BENCHMARK_PATTERN" ...`).
    * The raw timing results are saved to `$NEW_PATH/plots/timings/timings_old.out` and `timings_new.out`.
3.  **Plot Generation:**
    * It executes the `autobench.py` Python script.
    * This script parses both `.out` files, calculates the **median** timing for each benchmark, and computes the percentage change.
    * It generates a comparative bar chart and saves it as a PNG file in the `$NEW_PATH/plots/` directory with a timestamp (e.g., `plot-2025-11-03_10-30-00.png`).

-----

## Key Features & Notes

### Smart Benchmark Skipping

To save time during iterative development, the script will **skip** running the benchmarks for a specific version if **both** of the following conditions are met:

1.  The `make` command did not re-compile or re-link any C++ files (i.e., the build was already up-to-date).
2.  A corresponding timings file (e.g., `timings_new.out`) already exists from a previous run.

To force a re-run of the benchmarks, you can either `make clean` in the respective directory or simply delete the `timings_*.out` files from `$NEW_PATH/plots/timings/`.

### Benchmark Definition Consistency

The script *always* uses the benchmark definitions (`.benchmark` files) found within the **`NEW_PATH`** repository. 
This ensures a fair "apples-to-apples" comparison, as both versions are tested against the exact same benchmark logic (the one in your "new" branch).

### Output

All output is stored within the `$NEW_PATH/plots/` directory.

* **Raw Data:** 
  * `$NEW_PATH/plots/timings/timings_old.out`
  * `$NEW_PATH/plots/timings/timings_new.out`
* **Final Plot:** 
  * `$NEW_PATH/plots/plot-<YYYY-MM-DD_HH-MM-SS>.png`

The plot will show the "Old" vs. "New" median runtimes, with percentage change labels indicating performance regressions (🔺 red) or improvements (✅ green).

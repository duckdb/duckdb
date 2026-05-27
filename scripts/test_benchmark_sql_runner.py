import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import glob
import os
import subprocess
import sys


DEFAULT_BENCHMARK_DIRS = [
    "benchmark/imdb_plan_cost",
    "benchmark/tpch_plan_cost",
]


def run_sql_file(cli: str, db_path: str, sql_path: str, benchmark_dir: str) -> None:
    print(f"[{benchmark_dir}] RUNNING {sql_path}")
    with open(sql_path, "rb") as sql_file:
        subprocess.run([cli, db_path], stdin=sql_file, check=True, stdout=subprocess.DEVNULL)


def run_benchmark_dir(cli: str, benchmark_dir: str) -> None:
    benchmark_name = os.path.basename(os.path.normpath(benchmark_dir))
    db_path = f"{benchmark_name}.duckdb"

    try:
        run_sql_file(cli, db_path, os.path.join(benchmark_dir, "init", "schema.sql"), benchmark_dir)
        run_sql_file(cli, db_path, os.path.join(benchmark_dir, "init", "load.sql"), benchmark_dir)

        query_files = sorted(glob.glob(os.path.join(benchmark_dir, "queries", "*.sql")))
        for query_file in query_files:
            run_sql_file(cli, db_path, query_file, benchmark_dir)
    finally:
        if os.path.exists(db_path):
            os.remove(db_path)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run benchmark SQL files with DuckDB CLI to detect crashes.")
    parser.add_argument("--shell", type=str, required=True, help="Path to DuckDB CLI")
    parser.add_argument(
        "--benchmark-dir",
        action="append",
        default=[],
        help="Benchmark directory containing init/ and queries/ (can be passed multiple times)",
    )
    args = parser.parse_args()

    benchmark_dirs = args.benchmark_dir if args.benchmark_dir else DEFAULT_BENCHMARK_DIRS
    workers = min(len(benchmark_dirs), os.cpu_count() or 1)
    print(f"RUNNING {len(benchmark_dirs)} BENCHMARKS WITH {workers} THREADS")

    try:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {}
            for benchmark_dir in benchmark_dirs:
                print(f"START BENCHMARK {benchmark_dir}")
                futures[executor.submit(run_benchmark_dir, args.shell, benchmark_dir)] = benchmark_dir

            for future in as_completed(futures):
                benchmark_dir = futures[future]
                try:
                    future.result()
                    print(f"[{benchmark_dir}] DONE")
                except subprocess.CalledProcessError as exc:
                    print(
                        f"[{benchmark_dir}] FAILED: command '{exc.cmd}' returned exit code {exc.returncode}",
                        file=sys.stderr,
                    )
                    for pending_future in futures:
                        if pending_future is not future:
                            pending_future.cancel()
                    return exc.returncode
    except subprocess.CalledProcessError as exc:
        print(f"FAILED: command '{exc.cmd}' returned exit code {exc.returncode}", file=sys.stderr)
        return exc.returncode
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

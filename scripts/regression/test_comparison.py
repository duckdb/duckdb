import math
import os
import re
import stat
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from scripts.regression.comparison import (
    benchmark_measurement,
    confirmation_run_count,
    median_confidence_interval,
)


class TestBenchmarkComparison(unittest.TestCase):
    def test_median_confidence_interval(self):
        self.assertEqual(median_confidence_interval(list(range(1, 21))), (6, 15))
        self.assertEqual(median_confidence_interval([1.0] * 20), (1.0, 1.0))
        self.assertEqual(median_confidence_interval([1.0] * 4), (-math.inf, math.inf))

    def test_measurement_uses_paired_ratios_and_separate_medians(self):
        old = [1.0, 2.0, 100.0]
        new = [0.9, 1.8, 110.0]
        measurement = benchmark_measurement(old, new)
        self.assertEqual(measurement.old_timing, 2.0)
        self.assertEqual(measurement.new_timing, 1.8)
        self.assertAlmostEqual(measurement.ratio, 0.9)

    def test_measurement_validation(self):
        with self.assertRaises(ValueError):
            benchmark_measurement([], [])
        with self.assertRaises(ValueError):
            benchmark_measurement([1.0], [1.0, 2.0])
        with self.assertRaises(ValueError):
            benchmark_measurement([0.0], [1.0])
        with self.assertRaises(ValueError):
            benchmark_measurement([1.0], [math.inf])

    def test_confirmation_runs_scale_with_faster_side_median(self):
        cases = [
            (0.02, 100),
            (0.05, 60),
            (0.10, 30),
            (0.30, 15),
            (0.50, 15),
            (4.00, 15),
        ]
        for timing, expected_runs in cases:
            with self.subTest(timing=timing):
                measurement = benchmark_measurement([timing] * 10, [timing * 1.2] * 10)
                self.assertEqual(confirmation_run_count(measurement), expected_runs)


class TestRegressionRunnerIntegration(unittest.TestCase):
    repository_root = Path(__file__).resolve().parents[2]

    stable_runner_source = """#!/usr/bin/env python3
import os
import sys
from pathlib import Path

label = os.path.basename(sys.argv[0])
runs = int(sys.argv[sys.argv.index("--timed-runs") + 1])
with open(os.environ["BENCHMARK_ORDER_LOG"], "a", encoding="utf-8") as order_log:
    order_log.write(f"{label}:{runs}\\n")
cache_path = os.getenv("BENCHMARK_CACHE_PATH")
expected_cache_state = os.getenv("EXPECTED_BENCHMARK_CACHE_STATE")
if expected_cache_state:
    actual_cache_state = "present" if Path(cache_path).exists() else "absent"
    if actual_cache_state != expected_cache_state:
        print(f"expected benchmark cache to be {expected_cache_state}, found {actual_cache_state}", file=sys.stderr)
        raise SystemExit(1)
expected_memory_limit = os.getenv("EXPECTED_MEMORY_LIMIT")
if expected_memory_limit and f"--memory_limit={expected_memory_limit}" not in sys.argv:
    print("memory limit was not forwarded", file=sys.stderr)
    raise SystemExit(1)
timing = float(os.environ["BENCHMARK_NEW_TIMING"]) if label == "new" else 1.0
print("name\\trun\\ttiming", file=sys.stderr)
for run in range(1, runs + 1):
    print(f"fake.benchmark\\t{run}\\t{timing}", file=sys.stderr)
"""

    rejected_candidate_runner_source = """#!/usr/bin/env python3
import os
import sys
from pathlib import Path

label = os.path.basename(sys.argv[0])
runs = int(sys.argv[sys.argv.index("--timed-runs") + 1])
with open(os.environ["BENCHMARK_ORDER_LOG"], "a", encoding="utf-8") as order_log:
    order_log.write(f"{label}:{runs}\\n")
counter_path = Path(os.environ["BENCHMARK_COUNTER_DIR"]) / f"{label}.count"
invocation = int(counter_path.read_text(encoding="utf-8")) if counter_path.exists() else 0
counter_path.write_text(str(invocation + 1), encoding="utf-8")
timing = 1.3 if label == "new" and invocation < 2 else 1.0
print("name\\trun\\ttiming", file=sys.stderr)
for run in range(1, runs + 1):
    print(f"fake.benchmark\\t{run}\\t{timing}", file=sys.stderr)
"""

    inconclusive_candidate_runner_source = """#!/usr/bin/env python3
import os
import sys
from pathlib import Path

label = os.path.basename(sys.argv[0])
runs = int(sys.argv[sys.argv.index("--timed-runs") + 1])
with open(os.environ["BENCHMARK_ORDER_LOG"], "a", encoding="utf-8") as order_log:
    order_log.write(f"{label}:{runs}\\n")
counter_path = Path(os.environ["BENCHMARK_COUNTER_DIR"]) / f"{label}.count"
invocation = int(counter_path.read_text(encoding="utf-8")) if counter_path.exists() else 0
counter_path.write_text(str(invocation + 1), encoding="utf-8")
print("name\\trun\\ttiming", file=sys.stderr)
for run in range(1, runs + 1):
    if label == "old":
        timing = 1.0
    elif invocation < 2 or run % 2 == 0:
        timing = 1.3
    else:
        timing = 1.0
    print(f"fake.benchmark\\t{run}\\t{timing}", file=sys.stderr)
"""

    malformed_runner_source = """#!/usr/bin/env python3
import sys

print("name\\trun\\ttiming", file=sys.stderr)
print("not-a-valid-timing-row", file=sys.stderr)
"""

    def run_regression_test(
        self,
        runner_source,
        new_timing="0.95",
        extra_args=None,
        ci=False,
        create_cache=False,
        expected_cache_state=None,
        expected_memory_limit=None,
    ):
        with tempfile.TemporaryDirectory() as temp_directory:
            temp_path = Path(temp_directory)
            runner_paths = {
                "old": temp_path / "build" / "base" / "release" / "benchmark" / "old",
                "new": temp_path / "build" / "current" / "release" / "benchmark" / "new",
            }
            for label, runner_path in runner_paths.items():
                runner_path.parent.mkdir(parents=True, exist_ok=True)
                runner_path.write_text(runner_source, encoding="utf-8")
                runner_path.chmod(runner_path.stat().st_mode | stat.S_IXUSR)
            benchmark_list = temp_path / "benchmarks.csv"
            benchmark_list.write_text("fake.benchmark\n", encoding="utf-8")
            order_log = temp_path / "order.log"
            env = os.environ.copy()
            env["BENCHMARK_ORDER_LOG"] = str(order_log)
            env["BENCHMARK_COUNTER_DIR"] = str(temp_path)
            env["BENCHMARK_NEW_TIMING"] = new_timing
            if create_cache:
                cache_path = temp_path / "build" / "duckdb_benchmark_data"
                cache_path.mkdir()
                (cache_path / "marker").write_text("cached", encoding="utf-8")
                env["BENCHMARK_CACHE_PATH"] = str(cache_path)
            if expected_cache_state:
                env["EXPECTED_BENCHMARK_CACHE_STATE"] = expected_cache_state
            if expected_memory_limit:
                env["EXPECTED_MEMORY_LIMIT"] = expected_memory_limit
            if ci:
                env["CI"] = "true"

            command = [
                sys.executable,
                str(self.repository_root / "scripts/regression/test_runner.py"),
                "--old",
                str(runner_paths["old"]),
                "--new",
                str(runner_paths["new"]),
                "--benchmarks",
                str(benchmark_list),
                "--verbose",
            ]
            if extra_args:
                command.extend(extra_args)
            process = subprocess.run(
                command,
                cwd=self.repository_root,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=False,
            )
            order = order_log.read_text(encoding="utf-8").splitlines() if order_log.exists() else []
            return process, order

    def test_non_regression_stops_after_two_initial_batches(self):
        process, order = self.run_regression_test(self.stable_runner_source)
        self.assertEqual(process.returncode, 0, process.stdout + process.stderr)
        self.assertEqual(order, ["old:5", "new:5", "new:5", "old:5"])
        plain_output = re.sub(r"\x1b\[[0-9;]*m", "", process.stdout)
        self.assertIn("initial sampling: 10 runs per binary in 2 batches of 5", plain_output)
        self.assertIn("geomean (initial 10 samples): 1.000s -> 0.950s  -5.0%", plain_output)

    def test_stable_regression_is_confirmed_from_independent_samples(self):
        process, order = self.run_regression_test(self.stable_runner_source, new_timing="1.3")
        self.assertEqual(process.returncode, 1, process.stdout + process.stderr)
        self.assertEqual(
            order,
            ["old:5", "new:5", "new:5", "old:5", "old:8", "new:8", "new:7", "old:7"],
        )
        plain_output = re.sub(r"\x1b\[[0-9;]*m", "", process.stdout)
        self.assertIn("regression confirmation: fake.benchmark: 15 paired runs", plain_output)
        self.assertIn("95% CI for PR/base median ratio: 1.300x to 1.300x", plain_output)
        self.assertIn("regression limit: 1.100x (confirmed regression)", plain_output)
        self.assertRegex(plain_output, r"fake\s+1\.000s\s+1\.300s\s+\+0\.300s\s+\+30\.0%\s+10\+15")

    def test_nofail_reports_confirmed_regression_without_failing(self):
        process, _ = self.run_regression_test(self.stable_runner_source, new_timing="1.3", extra_args=["--nofail"])
        self.assertEqual(process.returncode, 0, process.stdout + process.stderr)
        self.assertIn("regression detected", process.stdout)
        self.assertIn("REGRESSIONS", process.stdout)

    def test_benchmark_cache_is_kept_by_default(self):
        process, _ = self.run_regression_test(
            self.stable_runner_source,
            create_cache=True,
            expected_cache_state="present",
        )
        self.assertEqual(process.returncode, 0, process.stdout + process.stderr)

    def test_benchmark_cache_can_be_cleared(self):
        process, _ = self.run_regression_test(
            self.stable_runner_source,
            extra_args=["--benchmark-cache=clear"],
            create_cache=True,
            expected_cache_state="absent",
        )
        self.assertEqual(process.returncode, 0, process.stdout + process.stderr)

    def test_memory_limit_is_forwarded_to_both_runners(self):
        process, _ = self.run_regression_test(
            self.stable_runner_source,
            extra_args=["--memory-limit", "512MB"],
            expected_memory_limit="512MB",
        )
        self.assertEqual(process.returncode, 0, process.stdout + process.stderr)

    def test_rejected_candidate_uses_confirmation_for_row_and_initial_for_geomean(self):
        process, order = self.run_regression_test(self.rejected_candidate_runner_source, ci=True)
        self.assertEqual(process.returncode, 0, process.stdout + process.stderr)
        self.assertEqual(
            order,
            ["old:5", "new:5", "new:5", "old:5", "old:8", "new:8", "new:7", "old:7"],
        )
        plain_output = re.sub(r"\x1b\[[0-9;]*m", "", process.stdout)
        self.assertIn("regression rejected", plain_output)
        self.assertIn("geomean (initial 10 samples): 1.000s -> 1.300s  +30.0%", plain_output)
        self.assertIn("INCONCLUSIVE REGRESSION CANDIDATES\n0 benchmarks", plain_output)
        self.assertNotIn("::warning title=Inconclusive regression benchmark::", plain_output)

    def test_inconclusive_candidate_warns_without_failing(self):
        process, _ = self.run_regression_test(self.inconclusive_candidate_runner_source, ci=True)
        self.assertEqual(process.returncode, 0, process.stdout + process.stderr)
        plain_output = re.sub(r"\x1b\[[0-9;]*m", "", process.stdout)
        self.assertIn("regression inconclusive", plain_output)
        self.assertIn("INCONCLUSIVE REGRESSION CANDIDATES", plain_output)
        self.assertIn("::warning title=Inconclusive regression benchmark::", plain_output)
        self.assertIn("10+15", plain_output)

    def test_malformed_runner_output_is_a_failure(self):
        process, _ = self.run_regression_test(self.malformed_runner_source)
        self.assertEqual(process.returncode, 1, process.stdout + process.stderr)
        self.assertIn("Could not parse benchmark timings", process.stdout)
        self.assertIn("benchmark failure detected", process.stdout)

    def test_removed_sampling_option_is_rejected(self):
        process, order = self.run_regression_test(self.stable_runner_source, extra_args=["--initial-runs", "20"])
        self.assertEqual(process.returncode, 2, process.stdout + process.stderr)
        self.assertIn("unrecognized arguments: --initial-runs 20", process.stderr)
        self.assertEqual(order, [])

    def test_removed_cache_flags_are_rejected(self):
        for removed_flag in ("--clear-benchmark-cache", "--keep-benchmark-data"):
            with self.subTest(removed_flag=removed_flag):
                process, order = self.run_regression_test(self.stable_runner_source, extra_args=[removed_flag])
                self.assertEqual(process.returncode, 2, process.stdout + process.stderr)
                self.assertIn(f"unrecognized arguments: {removed_flag}", process.stderr)
                self.assertEqual(order, [])


if __name__ == "__main__":
    unittest.main()

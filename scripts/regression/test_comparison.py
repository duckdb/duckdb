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
    sampling_batch_sizes,
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
            (0.30, 30),
            (0.50, 30),
            (4.00, 30),
        ]
        for timing, expected_runs in cases:
            with self.subTest(timing=timing):
                measurement = benchmark_measurement([timing] * 10, [timing * 1.2] * 10)
                self.assertEqual(confirmation_run_count(measurement), expected_runs)

    def test_sampling_batch_sizes(self):
        cases = [
            (1, [5]),
            (4, [5]),
            (5, [5]),
            (6, [5, 5]),
            (10, [5, 5]),
            (15, [5, 5, 5]),
            (84, [5] * 17),
            (100, [5] * 20),
        ]
        for requested_runs, expected_batches in cases:
            with self.subTest(requested_runs=requested_runs):
                self.assertEqual(sampling_batch_sizes(requested_runs), expected_batches)
        with self.assertRaises(ValueError):
            sampling_batch_sizes(0)


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
timing = (
    float(os.environ["BENCHMARK_NEW_TIMING"])
    if label == "new"
    else float(os.environ["BENCHMARK_OLD_TIMING"])
)
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
    elif invocation < 2 or run not in (3, 5):
        timing = 1.3
    else:
        timing = 1.0
    print(f"fake.benchmark\\t{run}\\t{timing}", file=sys.stderr)
"""

    noisy_improvement_runner_source = """#!/usr/bin/env python3
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
    elif invocation < 2 or run not in (3, 5):
        timing = 0.95
    else:
        timing = 1.0
    print(f"fake.benchmark\\t{run}\\t{timing}", file=sys.stderr)
"""

    early_stop_after_twenty_runner_source = """#!/usr/bin/env python3
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
    elif invocation < 2:
        timing = 1.3
    elif invocation < 4 and run == 1:
        timing = 0.9
    elif invocation < 4 and run == 2:
        timing = 1.1
    else:
        timing = 1.0
    print(f"fake.benchmark\\t{run}\\t{timing}", file=sys.stderr)
"""

    wide_noise_runner_source = """#!/usr/bin/env python3
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
    elif invocation < 2:
        timing = 1.3
    elif run <= 2:
        timing = 0.9
    elif run == 3:
        timing = 1.0
    else:
        timing = 1.1
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
        new_timing="1.0",
        old_timing="1.0",
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
            env["BENCHMARK_OLD_TIMING"] = old_timing
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

    @staticmethod
    def expected_order(confirmation_runs=0):
        order = ["old:5", "new:5", "old:5", "new:5"]
        if not confirmation_runs:
            return order
        for batch_size in sampling_batch_sizes(confirmation_runs):
            order.extend([f"old:{batch_size}", f"new:{batch_size}"])
        return order

    def test_non_regression_stops_after_two_initial_batches(self):
        process, order = self.run_regression_test(self.stable_runner_source)
        self.assertEqual(process.returncode, 0, process.stdout + process.stderr)
        self.assertEqual(order, self.expected_order())
        plain_output = re.sub(r"\x1b\[[0-9;]*m", "", process.stdout)
        self.assertNotIn("BENCHMARK QUERY RESULTS", plain_output)
        self.assertIn("suite: 1 benchmarks", plain_output)
        self.assertIn(
            "sampling: 10 initial pairs; 30–100 confirmation pairs outside ±2%, with early noise stop",
            plain_output,
        )
        self.assertIn("regression: fail when the 95% CI is above +10.0%", plain_output)
        self.assertIn("median change +0.0% | within ±2%; done", plain_output)
        self.assertIn("geomean: 1.000s -> 1.000s  +0.0%  (initial 10 samples)\nresult: no regressions", plain_output)
        self.assertIn("UNCHANGED (±2%)\n1 benchmarks", plain_output)
        self.assertNotIn("\nFASTER\n", plain_output)
        self.assertNotIn("\nSLOWER (+2%…+10%)\n", plain_output)
        self.assertNotIn("\nUNCERTAIN REGRESSIONS\n", plain_output)
        self.assertNotIn("\nREGRESSIONS\n", plain_output)
        self.assertLess(plain_output.index("UNCHANGED (±2%)"), plain_output.index("geomean:"))
        self.assertTrue(plain_output.rstrip().endswith("result: no regressions"))
        self.assertIn("\033[90msuite: 1 benchmarks\033[0m", process.stdout)
        self.assertIn(
            "\033[90msampling: 10 initial pairs; 30–100 confirmation pairs outside ±2%, "
            "with early noise stop\033[0m",
            process.stdout,
        )
        self.assertIn("\033[90mregression: fail when the 95% CI is above +10.0%\033[0m", process.stdout)
        self.assertIn("\033[90mUNCHANGED (±2%)\033[0m\n\033[90m1 benchmarks\033[0m", process.stdout)
        self.assertIn("  +0.0%  \033[90m(initial 10 samples)\033[0m", process.stdout)
        self.assertIn("result: \033[32mno regressions\033[0m", process.stdout)

    def test_noise_boundaries_do_not_receive_confirmation_samples(self):
        for new_timing in ("0.98", "1.02"):
            with self.subTest(new_timing=new_timing):
                process, order = self.run_regression_test(self.stable_runner_source, new_timing=new_timing)
                self.assertEqual(process.returncode, 0, process.stdout + process.stderr)
                self.assertEqual(order, self.expected_order())

    def test_within_noise_geomean_uses_sign_color(self):
        cases = [
            ("0.999", "\033[32m-0.1%\033[0m"),
            ("1.001", "\033[31m+0.1%\033[0m"),
        ]
        for new_timing, colored_change in cases:
            with self.subTest(new_timing=new_timing):
                process, order = self.run_regression_test(self.stable_runner_source, new_timing=new_timing)
                self.assertEqual(process.returncode, 0, process.stdout + process.stderr)
                self.assertEqual(order, self.expected_order())
                self.assertIn(
                    f"{colored_change}  \033[90m(initial 10 samples)\033[0m",
                    process.stdout,
                )

    def test_improvement_receives_confirmation_and_is_classified_by_median(self):
        process, order = self.run_regression_test(self.noisy_improvement_runner_source)
        self.assertEqual(process.returncode, 0, process.stdout + process.stderr)
        self.assertEqual(order, self.expected_order(30))
        plain_output = re.sub(r"\x1b\[[0-9;]*m", "", process.stdout)
        self.assertIn(
            "initial: fake.benchmark: 10 pairs | median change -5.0% | confirming with 30 pairs", plain_output
        )
        self.assertIn(
            "confirm: fake.benchmark: 30 pairs | median change -5.0% | 95% CI -5.0%…+0.0% | "
            "faster (uncertain; budget exhausted)",
            plain_output,
        )
        self.assertRegex(
            plain_output,
            r"FASTER\nbenchmark[^\n]*\n-+[^\n]*\nfake\s+1\.000s\s+0\.950s\s+-0\.050s\s+-5\.0%\s+"
            r"-5\.0%…\+0\.0%\s+uncertain\s+10\+30",
        )
        self.assertIn("geomean: 1.000s -> 0.950s  -5.0%  (initial 10 samples)", plain_output)
        self.assertIn("\033[32m-5.0%\033[0m  \033[90m(initial 10 samples)\033[0m", process.stdout)

    def test_slowdown_below_regression_limit_receives_confirmation(self):
        process, order = self.run_regression_test(self.stable_runner_source, new_timing="1.08", ci=True)
        self.assertEqual(process.returncode, 0, process.stdout + process.stderr)
        self.assertEqual(order, self.expected_order(30))
        plain_output = re.sub(r"\x1b\[[0-9;]*m", "", process.stdout)
        self.assertIn("median change +8.0% | 95% CI +8.0%…+8.0% | slower (confident)", plain_output)
        self.assertRegex(
            plain_output,
            r"SLOWER \(\+2%…\+10%\)\nbenchmark[^\n]*\n-+[^\n]*\nfake\s+1\.000s\s+1\.080s\s+"
            r"\+0\.080s\s+\+8\.0%\s+\+8\.0%…\+8\.0%\s+confident\s+10\+30",
        )
        self.assertNotIn("::warning title=Inconclusive regression benchmark::", plain_output)
        self.assertIn("\033[31m+8.0%\033[0m  \033[90m(initial 10 samples)\033[0m", process.stdout)

    def test_stable_regression_is_confirmed_from_independent_samples(self):
        process, order = self.run_regression_test(self.stable_runner_source, new_timing="1.3")
        self.assertEqual(process.returncode, 1, process.stdout + process.stderr)
        self.assertEqual(order, self.expected_order(30))
        plain_output = re.sub(r"\x1b\[[0-9;]*m", "", process.stdout)
        self.assertIn(
            "confirm: fake.benchmark: 30 pairs | median change +30.0% | 95% CI +30.0%…+30.0% | "
            "regression (confirmed)",
            plain_output,
        )
        self.assertRegex(
            plain_output,
            r"fake\s+1\.000s\s+1\.300s\s+\+0\.300s\s+\+30\.0%\s+\+30\.0%…\+30\.0%\s+" r"confirmed regression\s+10\+30",
        )
        self.assertIn("result: \033[31mregression detected\033[0m", process.stdout)

    def test_confirmation_uses_fixed_five_run_batches_and_rounds_up(self):
        process, order = self.run_regression_test(
            self.stable_runner_source,
            old_timing="0.036",
            new_timing="0.045",
        )
        self.assertEqual(process.returncode, 1, process.stdout + process.stderr)
        self.assertEqual(order, self.expected_order(85))
        self.assertTrue(all(entry.endswith(":5") for entry in order))
        plain_output = re.sub(r"\x1b\[[0-9;]*m", "", process.stdout)
        self.assertIn(
            "initial: fake.benchmark: 10 pairs | median change +25.0% | confirming with 85 pairs",
            plain_output,
        )
        self.assertIn("confirm: fake.benchmark: 85 pairs", plain_output)
        self.assertIn("10+85", plain_output)

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
        self.assertEqual(order, self.expected_order(10))
        plain_output = re.sub(r"\x1b\[[0-9;]*m", "", process.stdout)
        self.assertIn("within ±2% (confident; stopped early)", plain_output)
        self.assertIn("geomean: 1.000s -> 1.300s  +30.0%  (initial 10 samples)", plain_output)
        self.assertNotIn("UNCERTAIN REGRESSIONS", plain_output)
        self.assertNotIn("::warning title=Inconclusive regression benchmark::", plain_output)

    def test_confirmation_stops_after_twenty_pairs_when_noise_ci_becomes_confident(self):
        process, order = self.run_regression_test(self.early_stop_after_twenty_runner_source)
        self.assertEqual(process.returncode, 0, process.stdout + process.stderr)
        self.assertEqual(order, self.expected_order(20))
        plain_output = re.sub(r"\x1b\[[0-9;]*m", "", process.stdout)
        self.assertIn(
            "confirm: fake.benchmark: 20 pairs | median change +0.0% | 95% CI +0.0%…+0.0% | "
            "within ±2% (confident; stopped early)",
            plain_output,
        )

    def test_confirmation_does_not_stop_when_median_is_noise_but_ci_is_wide(self):
        process, order = self.run_regression_test(self.wide_noise_runner_source)
        self.assertEqual(process.returncode, 0, process.stdout + process.stderr)
        self.assertEqual(order, self.expected_order(30))
        plain_output = re.sub(r"\x1b\[[0-9;]*m", "", process.stdout)
        self.assertIn(
            "confirm: fake.benchmark: 30 pairs | median change +0.0% | 95% CI -10.0%…+10.0% | " "within ±2%; no change",
            plain_output,
        )

    def test_inconclusive_candidate_warns_without_failing(self):
        process, _ = self.run_regression_test(self.inconclusive_candidate_runner_source, ci=True)
        self.assertEqual(process.returncode, 0, process.stdout + process.stderr)
        plain_output = re.sub(r"\x1b\[[0-9;]*m", "", process.stdout)
        self.assertIn("regression (uncertain; budget exhausted)", plain_output)
        self.assertIn("UNCERTAIN REGRESSIONS", plain_output)
        self.assertIn("::warning title=Inconclusive regression benchmark::", plain_output)
        self.assertIn("10+30", plain_output)
        self.assertIn("result: \033[33muncertain regressions\033[0m", process.stdout)
        self.assertLess(plain_output.index("UNCERTAIN REGRESSIONS"), plain_output.index("geomean:"))
        self.assertTrue(plain_output.rstrip().endswith("result: uncertain regressions"))

    def test_malformed_runner_output_is_a_failure(self):
        process, _ = self.run_regression_test(self.malformed_runner_source)
        self.assertEqual(process.returncode, 1, process.stdout + process.stderr)
        self.assertIn("Could not parse benchmark timings", process.stdout)
        self.assertIn("benchmark failure", process.stdout)
        self.assertIn("result: \033[31mbenchmark failure\033[0m", process.stdout)
        plain_output = re.sub(r"\x1b\[[0-9;]*m", "", process.stdout)
        self.assertLess(plain_output.index("FAILURES SUMMARY"), plain_output.index("geomean:"))
        self.assertTrue(plain_output.rstrip().endswith("result: benchmark failure"))

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

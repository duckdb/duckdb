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
    confirmation_run_count,
    median_confidence_interval,
    paired_measurement,
    regression_measurement,
)


class TestPairedComparison(unittest.TestCase):
    def test_median_confidence_interval(self):
        lower, upper = median_confidence_interval(list(range(1, 21)))
        self.assertEqual((lower, upper), (6, 15))
        self.assertEqual(median_confidence_interval([1.0] * 20), (1.0, 1.0))
        self.assertEqual(median_confidence_interval([1.0] * 6), (1.0, 1.0))
        self.assertEqual(median_confidence_interval([1.0] * 4), (-math.inf, math.inf))

    def test_q53_uses_paired_change_instead_of_pooled_medians(self):
        old = [
            0.015359,
            0.015292,
            0.014707,
            0.014988,
            0.014316,
            0.015348,
            0.014787,
            0.014339,
            0.014135,
            0.014171,
            0.016209,
            0.015087,
            0.015156,
            0.014618,
            0.014771,
            0.016077,
            0.014951,
            0.015016,
            0.014579,
            0.014426,
            0.020085,
            0.019983,
            0.021580,
            0.021364,
            0.022356,
            0.021520,
            0.021329,
            0.019311,
            0.020324,
            0.024281,
        ]
        new = [
            0.016120,
            0.014158,
            0.014453,
            0.014663,
            0.013699,
            0.017042,
            0.014953,
            0.013993,
            0.013678,
            0.013685,
            0.015336,
            0.014745,
            0.014687,
            0.013690,
            0.013658,
            0.024608,
            0.021533,
            0.023831,
            0.022940,
            0.021340,
            0.021216,
            0.019005,
            0.020576,
            0.019424,
            0.022571,
            0.021240,
            0.019692,
            0.018916,
            0.020069,
            0.019921,
        ]

        measurement = paired_measurement(old, new)
        pooled_new = sum(sorted(new)[14:16]) / 2
        pooled_old = sum(sorted(old)[14:16]) / 2
        self.assertAlmostEqual((pooled_new / pooled_old - 1.0) * 100.0, 18.1, places=1)
        self.assertAlmostEqual((measurement.ratio - 1.0) * 100.0, -2.2, places=1)

    def test_regression_measurement_includes_both_thresholds(self):
        below_threshold = regression_measurement([1.0] * 10, [1.15] * 10, 0.1, 0.05)
        above_threshold = regression_measurement([1.0] * 10, [1.20] * 10, 0.1, 0.05)
        self.assertLess(below_threshold.ratio, 1.0)
        self.assertGreater(above_threshold.ratio, 1.0)
        self.assertGreater(above_threshold.ratio_interval[0], 1.0)

    def test_confirmation_runs_scale_with_runtime(self):
        cases = [
            (0.02, 100),
            (0.05, 60),
            (0.1, 30),
            (0.3, 15),
            (0.5, 15),
            (4.0, 15),
        ]
        for timing, expected_runs in cases:
            with self.subTest(timing=timing):
                measurement = paired_measurement([timing] * 10, [timing * 1.01] * 10)
                self.assertEqual(confirmation_run_count(measurement, 3.0, 15, 100), expected_runs)

    def test_confirmation_run_validation(self):
        measurement = paired_measurement([1.0] * 10, [1.0] * 10)
        with self.assertRaises(ValueError):
            confirmation_run_count(measurement, 0.0, 6, 100)
        with self.assertRaises(ValueError):
            confirmation_run_count(measurement, 3.0, 10, 6)


class TestRegressionRunnerIntegration(unittest.TestCase):
    repository_root = Path(__file__).resolve().parents[2]

    runner_source = """#!/usr/bin/env python3
import os
import sys

label = os.path.basename(sys.argv[0])
if "--help" in sys.argv:
    print("--timed-runs")
    raise SystemExit(0)
runs = int(sys.argv[sys.argv.index("--timed-runs") + 1])
with open(os.environ["BENCHMARK_ORDER_LOG"], "a", encoding="utf-8") as order_log:
    order_log.write(label + "\\n")
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
if "--help" in sys.argv:
    print("--timed-runs")
    raise SystemExit(0)
runs = int(sys.argv[sys.argv.index("--timed-runs") + 1])
with open(os.environ["BENCHMARK_ORDER_LOG"], "a", encoding="utf-8") as order_log:
    order_log.write(label + "\\n")
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
if "--help" in sys.argv:
    print("--timed-runs")
    raise SystemExit(0)
runs = int(sys.argv[sys.argv.index("--timed-runs") + 1])
with open(os.environ["BENCHMARK_ORDER_LOG"], "a", encoding="utf-8") as order_log:
    order_log.write(label + "\\n")
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

    def run_regression_test(self, runner_source, new_timing="0.95", extra_args=None, ci=False):
        with tempfile.TemporaryDirectory() as temp_directory:
            temp_path = Path(temp_directory)
            for label in ("old", "new"):
                runner_path = temp_path / label
                runner_path.write_text(runner_source, encoding="utf-8")
                runner_path.chmod(runner_path.stat().st_mode | stat.S_IXUSR)
            benchmark_list = temp_path / "benchmarks.csv"
            benchmark_list.write_text("fake.benchmark\n", encoding="utf-8")
            order_log = temp_path / "order.log"
            env = os.environ.copy()
            env["BENCHMARK_ORDER_LOG"] = str(order_log)
            env["BENCHMARK_COUNTER_DIR"] = str(temp_path)
            env["BENCHMARK_NEW_TIMING"] = new_timing
            if ci:
                env["CI"] = "true"

            command = [
                sys.executable,
                str(self.repository_root / "scripts/regression/test_runner.py"),
                "--old",
                str(temp_path / "old"),
                "--new",
                str(temp_path / "new"),
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

    def test_non_regression_stops_after_initial_runs(self):
        process, order = self.run_regression_test(self.runner_source)
        self.assertEqual(process.returncode, 0, process.stdout + process.stderr)
        self.assertEqual(order, ["old", "new", "new", "old"])
        plain_output = re.sub(r"\x1b\[[0-9;]*m", "", process.stdout)
        self.assertIn(
            "timing: 10 initial runs; regression confirmation targets 3s",
            plain_output,
        )
        self.assertRegex(plain_output, r"fake\s+1\.000s\s+0\.950s\s+-0\.050s\s+-5\.0%\s+10")

    def test_stable_regression_is_confirmed(self):
        process, order = self.run_regression_test(self.runner_source, new_timing="1.3")
        self.assertEqual(process.returncode, 1, process.stdout + process.stderr)
        self.assertEqual(order, ["old", "new", "new", "old", "old", "new", "new", "old"])
        plain_output = re.sub(r"\x1b\[[0-9;]*m", "", process.stdout)
        self.assertIn("regression confirmation: fake.benchmark: 15 paired runs", plain_output)
        self.assertIn("confirmed regression", plain_output)
        self.assertRegex(plain_output, r"fake\s+1\.000s\s+1\.300s\s+\+0\.300s\s+\+30\.0%\s+10\+15")

    def test_rejected_regression_candidate_does_not_warn(self):
        process, order = self.run_regression_test(self.rejected_candidate_runner_source, ci=True)
        self.assertEqual(process.returncode, 0, process.stdout + process.stderr)
        self.assertEqual(order, ["old", "new", "new", "old", "old", "new", "new", "old"])
        plain_output = re.sub(r"\x1b\[[0-9;]*m", "", process.stdout)
        self.assertIn("regression rejected", plain_output)
        self.assertIn("UNCONFIRMED REGRESSION CANDIDATES\n0 benchmarks", plain_output)
        self.assertNotIn("::warning title=Unconfirmed regression benchmark::", plain_output)

    def test_inconclusive_regression_candidate_warns(self):
        process, order = self.run_regression_test(self.inconclusive_candidate_runner_source, ci=True)
        self.assertEqual(process.returncode, 0, process.stdout + process.stderr)
        self.assertEqual(order, ["old", "new", "new", "old", "old", "new", "new", "old"])
        plain_output = re.sub(r"\x1b\[[0-9;]*m", "", process.stdout)
        self.assertIn("regression inconclusive", plain_output)
        self.assertIn("UNCONFIRMED REGRESSION CANDIDATES", plain_output)
        self.assertIn("::warning title=Unconfirmed regression benchmark::", plain_output)
        self.assertIn("10+15", plain_output)

    def test_runner_without_timed_run_support_fails_early(self):
        runner_source = """#!/usr/bin/env python3
import os
import sys

label = os.path.basename(sys.argv[0])
if "--help" in sys.argv:
    print("legacy benchmark runner")
    raise SystemExit(0)
with open(os.environ["BENCHMARK_ORDER_LOG"], "a", encoding="utf-8") as order_log:
    order_log.write(label + "\\n")
"""
        process, order = self.run_regression_test(runner_source)
        self.assertEqual(process.returncode, 1, process.stdout + process.stderr)
        self.assertIn("requires both benchmark runners to support --timed-runs", process.stdout)
        self.assertEqual(order, [])


if __name__ == "__main__":
    unittest.main()

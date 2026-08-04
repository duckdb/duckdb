import math
import os
import re
import stat
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from scripts.regression.comparison import median_confidence_interval, paired_measurement, sampling_decision


class TestPairedComparison(unittest.TestCase):
    def make_decision(self, measurement, measured_seconds=1.0, minimum_runs=20, maximum_runs=100):
        return sampling_decision(
            measurement,
            measured_seconds=measured_seconds,
            minimum_runs=minimum_runs,
            maximum_runs=maximum_runs,
            maximum_adaptive_seconds=10.0,
            display_threshold_percentage=2.0,
            regression_threshold_percentage=0.1,
            regression_threshold_seconds=0.05,
        )

    def test_median_confidence_interval(self):
        lower, upper = median_confidence_interval(list(range(1, 21)))
        self.assertEqual((lower, upper), (6, 15))
        self.assertEqual(median_confidence_interval([1.0] * 20), (1.0, 1.0))
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
        pooled_change = sorted(new)[14:16]
        pooled_new = sum(pooled_change) / 2
        pooled_old = sum(sorted(old)[14:16]) / 2
        self.assertAlmostEqual((pooled_new / pooled_old - 1.0) * 100.0, 18.1, places=1)
        self.assertAlmostEqual((measurement.ratio - 1.0) * 100.0, -2.2, places=1)
        self.assertTrue(self.make_decision(measurement).collect_more)

    def test_close_results_stop_at_the_minimum(self):
        measurement = paired_measurement([1.0] * 20, [1.01] * 20)
        decision = self.make_decision(measurement)
        self.assertFalse(decision.collect_more)
        self.assertIn("display threshold", decision.reason)

    def test_stable_visible_results_stop(self):
        improvement = paired_measurement([1.0] * 20, [0.95] * 20)
        slowdown = paired_measurement([1.0] * 20, [1.05] * 20)
        regression = paired_measurement([1.0] * 20, [1.20] * 20)
        self.assertFalse(self.make_decision(improvement).collect_more)
        self.assertFalse(self.make_decision(slowdown).collect_more)
        self.assertFalse(self.make_decision(regression).collect_more)

    def test_minimum_run_time_and_sample_caps(self):
        measurement = paired_measurement([1.0] * 10, [1.05] * 10)
        self.assertTrue(self.make_decision(measurement, measured_seconds=20.0).collect_more)

        measurement = paired_measurement([1.0] * 20, [1.01, 1.10] * 10)
        time_decision = self.make_decision(measurement, measured_seconds=10.0)
        self.assertFalse(time_decision.collect_more)
        self.assertIn("budget", time_decision.reason)

        measurement = paired_measurement([1.0] * 100, [1.01, 1.10] * 50)
        run_decision = self.make_decision(measurement, maximum_runs=100)
        self.assertFalse(run_decision.collect_more)
        self.assertIn("maximum timed runs", run_decision.reason)


class TestRegressionRunnerIntegration(unittest.TestCase):
    def test_adaptive_runner_alternates_batches_and_reports_runs(self):
        repository_root = Path(__file__).resolve().parents[2]
        runner_source = '''#!/usr/bin/env python3
import os
import sys

label = os.path.basename(sys.argv[0])
if "--help" in sys.argv:
    print("--timed-runs")
    raise SystemExit(0)
runs = 5
if "--timed-runs" in sys.argv:
    runs = int(sys.argv[sys.argv.index("--timed-runs") + 1])
with open(os.environ["BENCHMARK_ORDER_LOG"], "a", encoding="utf-8") as order_log:
    order_log.write(label + "\\n")
timing = 0.95 if label == "new" else 1.0
print("name\\trun\\ttiming", file=sys.stderr)
for run in range(1, runs + 1):
    print(f"fake.benchmark\\t{run}\\t{timing}", file=sys.stderr)
'''
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

            process = subprocess.run(
                [
                    sys.executable,
                    str(repository_root / "scripts/regression/test_runner.py"),
                    "--old",
                    str(temp_path / "old"),
                    "--new",
                    str(temp_path / "new"),
                    "--benchmarks",
                    str(benchmark_list),
                ],
                cwd=repository_root,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=False,
            )

            self.assertEqual(process.returncode, 0, process.stdout + process.stderr)
            self.assertEqual(
                order_log.read_text(encoding="utf-8").splitlines(),
                ["old", "new", "new", "old", "old", "new", "new", "old"],
            )
            plain_output = re.sub(r"\x1b\[[0-9;]*m", "", process.stdout)
            self.assertIn("paired median, 20-100 timed runs", plain_output)
            self.assertRegex(plain_output, r"fake\s+1\.000s\s+0\.950s\s+-0\.050s\s+-5\.0%\s+20")

    def test_runner_without_timed_run_support_uses_legacy_sampling(self):
        repository_root = Path(__file__).resolve().parents[2]
        runner_source = '''#!/usr/bin/env python3
import os
import sys

label = os.path.basename(sys.argv[0])
if "--help" in sys.argv:
    print("legacy benchmark runner")
    raise SystemExit(0)
with open(os.environ["BENCHMARK_ORDER_LOG"], "a", encoding="utf-8") as order_log:
    order_log.write(label + "\\n")
timing = 0.95 if label == "new" else 1.0
print("name\\trun\\ttiming", file=sys.stderr)
for run in range(1, 6):
    print(f"fake.benchmark\\t{run}\\t{timing}", file=sys.stderr)
'''
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

            process = subprocess.run(
                [
                    sys.executable,
                    str(repository_root / "scripts/regression/test_runner.py"),
                    "--old",
                    str(temp_path / "old"),
                    "--new",
                    str(temp_path / "new"),
                    "--benchmarks",
                    str(benchmark_list),
                    "--timed-runs",
                    "20",
                ],
                cwd=repository_root,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=False,
            )

            self.assertEqual(process.returncode, 0, process.stdout + process.stderr)
            self.assertIn("Adaptive paired sampling disabled", process.stdout)
            self.assertEqual(
                order_log.read_text(encoding="utf-8").splitlines(),
                ["old", "new", "old", "new", "old", "new", "old", "new"],
            )
            self.assertIn("timing: median of 20 timed runs", process.stdout)


if __name__ == "__main__":
    unittest.main()

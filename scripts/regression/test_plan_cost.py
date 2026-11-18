import contextlib
import io
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from scripts import plan_cost_runner
from scripts.plan_cost_runner import is_measured_join, op_inspect


def scan(rows):
    return {'type': 'TABLE_SCAN', 'intermediate_rows': rows, 'children': []}


def join(join_type, rows, left, right):
    return {
        'type': 'HASH_JOIN',
        'intermediate_rows': rows,
        'extra_info': {'Join Type': join_type},
        'children': [left, right],
    }


class TestPlanCost(unittest.TestCase):
    def test_membership_join_types_are_measured(self):
        for join_type in ('INNER', 'MARK', 'SEMI', 'ANTI', 'RIGHT_SEMI', 'RIGHT_ANTI'):
            with self.subTest(join_type=join_type):
                self.assertTrue(is_measured_join(join(join_type, 5, scan(20), scan(10))))

    def test_membership_rewrite_preserves_input_costs(self):
        inner = join('INNER', 20, scan(100), scan(8))
        mark = join('MARK', 20, inner, scan(10))
        filtered_mark = {'type': 'FILTER', 'intermediate_rows': 5, 'children': [mark]}
        old = op_inspect(filtered_mark)
        for join_type in ('SEMI', 'ANTI'):
            with self.subTest(join_type=join_type):
                new = op_inspect(join(join_type, 5, inner, scan(10)))
                self.assertEqual((old.total, old.build_side, old.probe_side), (40, 18, 120))
                self.assertEqual((new.total, new.build_side, new.probe_side), (25, 18, 120))
                self.assertFalse(new > old)

    def test_more_intermediate_and_build_rows_still_regress(self):
        old = op_inspect(join('MARK', 20, scan(20), scan(10)))
        new = op_inspect(join('SEMI', 30, scan(100), scan(40)))
        self.assertGreater(new, old)

    def test_lower_output_with_more_join_work_uses_timing(self):
        old = op_inspect(join('MARK', 20, scan(20), scan(10)))
        old.time = 1.0
        for join_type in ('SEMI', 'ANTI'):
            for probe_rows, build_rows in ((20, 40), (100, 10), (100, 40)):
                with self.subTest(join_type=join_type, probe_rows=probe_rows, build_rows=build_rows):
                    new = op_inspect(join(join_type, 5, scan(probe_rows), scan(build_rows)))
                    new.time = 1.1
                    self.assertGreater(new, old)
                    for runtime in (0.9, 1.02):
                        new.time = runtime
                        self.assertFalse(new > old)

    def test_lower_output_without_more_join_work_does_not_regress(self):
        old = op_inspect(join('MARK', 20, scan(20), scan(10)))
        new = op_inspect(join('SEMI', 5, scan(20), scan(10)))
        old.time = 1.0
        new.time = 2.0
        self.assertFalse(new > old)

    def test_mixed_costs_have_consistent_comparisons(self):
        old = op_inspect(join('MARK', 20, scan(20), scan(40)))
        new = op_inspect(join('SEMI', 5, scan(100), scan(10)))
        old.time = 1.0
        new.time = 1.1
        self.assertGreater(new, old)
        self.assertFalse(old > new)
        self.assertLess(old, new)
        self.assertFalse(new < old)
        new.time = 1.02
        self.assertFalse(old > new or new > old or old < new or new < old)

    def test_profiled_join_work_regression_reaches_runner(self):
        for time_format in ('query', 'latency', 'timing'):
            for old_build, new_build in ((10, 40), (40, 10)):
                with self.subTest(time_format=time_format, old_build=old_build):
                    profiles = []
                    for tree, elapsed in (
                        (join('MARK', 20, scan(20), scan(old_build)), 1.0),
                        (join('SEMI', 5, scan(100), scan(new_build)), 1.1),
                    ):
                        if time_format == 'query':
                            profiles.append({'operator': [tree], 'query': {'total_time': elapsed}})
                        else:
                            profiles.append({'children': [tree], time_format: elapsed})
                    with tempfile.TemporaryDirectory() as directory:
                        root = Path(directory)
                        (root / 'queries').mkdir()
                        (root / 'queries/q.sql').write_text('SELECT 1;')
                        profile_path = root / 'profile.json'
                        pending = iter(profiles)

                        def run_command(command, **kwargs):
                            profile_path.write_text(json.dumps(next(pending)))
                            return subprocess.CompletedProcess(command, 0)

                        def init_db(cli, dbname, benchmark_dir, extensions):
                            Path(dbname).touch()

                        output = io.StringIO()
                        with contextlib.ExitStack() as stack:
                            stack.enter_context(patch.object(plan_cost_runner, 'run_command', side_effect=run_command))
                            stack.enter_context(patch.object(plan_cost_runner, 'init_db', side_effect=init_db))
                            for name, value in (
                                ('OLD_DB_NAME', root / 'old.duckdb'),
                                ('NEW_DB_NAME', root / 'new.duckdb'),
                                ('PROFILE_FILENAME', profile_path),
                            ):
                                stack.enter_context(patch.object(plan_cost_runner, name, str(value)))
                            stack.enter_context(
                                patch('sys.argv', ['runner', '--old', 'old', '--new', 'new', '--dir', directory])
                            )
                            stack.enter_context(contextlib.redirect_stdout(output))
                            with self.assertRaises(SystemExit) as result:
                                plan_cost_runner.main()
                        self.assertEqual(result.exception.code, 1)
                        self.assertIn('REGRESSIONS DETECTED', output.getvalue())
                        self.assertNotIn('IMPROVEMENTS DETECTED', output.getvalue())


if __name__ == '__main__':
    unittest.main()

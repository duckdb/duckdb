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
from scripts.regression.comparison import benchmark_measurement


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

    def test_timing_is_required_independently_of_cold_latency(self):
        old = op_inspect(join('MARK', 20, scan(20), scan(10)))
        for rows, probe, build, expected in ((20, 20, 10, False), (30, 30, 20, False), (5, 100, 40, True)):
            new = op_inspect(join('SEMI', rows, scan(probe), scan(build)))
            for old_time, new_time in ((1.0, 2.0), (2.0, 1.0)):
                old.time, new.time = old_time, new_time
                self.assertEqual(old.needs_timing(new), expected)
                self.assertEqual(new.needs_timing(old), expected)

    def test_query_timing_batch_warms_up_before_measuring(self):
        completed = subprocess.CompletedProcess(
            [],
            0,
            stdout='Run Time (s): real 1.000000 user 0 sys 0\nRun Time (s): real 2.000000 user 0 sys 0\n',
            stderr='',
        )
        with patch.object(plan_cost_runner, 'run_command', return_value=completed) as run:
            self.assertEqual(plan_cost_runner.query_timings('cli', 'db', 'SELECT 42', 2), [1.0, 2.0])
        command = run.call_args.args[0]
        self.assertEqual(command[3:], ['-c', 'SELECT 42', '-c', '.timer on 6', '-c', 'SELECT 42', '-c', 'SELECT 42'])

    def test_query_timing_rejects_failure_or_missing_samples(self):
        for completed in (
            subprocess.CompletedProcess([], 1, stdout='', stderr='query failed'),
            subprocess.CompletedProcess([], 0, stdout='', stderr=''),
            subprocess.CompletedProcess([], 0, stdout='Run Time (s): real 1.0\n', stderr=''),
        ):
            with patch.object(plan_cost_runner, 'run_command', return_value=completed):
                with self.assertRaises(RuntimeError):
                    plan_cost_runner.query_timings('cli', 'db', 'SELECT 42', 2)

    def test_timing_batches_alternate_and_confirm_independently(self):
        for initial_ratio, confirmed_ratio in ((1.1, 1.0), (1.1, 1.1), (0.9, 1.0)):
            calls = []

            def timings(cli, dbname, query, runs):
                ratio = initial_ratio if len(calls) < 4 else confirmed_ratio
                calls.append(cli)
                return [1.0 if cli == 'old' else ratio] * runs

            with patch.object(plan_cost_runner, 'query_timings', side_effect=timings):
                measured = plan_cost_runner.measure_query_timings('old', 'new', 'SELECT 42')
            self.assertEqual(measured.ratio, confirmed_ratio)
            self.assertEqual(measured.runs, 30)
            self.assertEqual(calls, ['old', 'new', 'new', 'old'] * 4)

    def test_timings_within_noise_do_not_need_confirmation(self):
        with patch.object(plan_cost_runner, 'query_timings', return_value=[1.0] * 5) as timings:
            measured = plan_cost_runner.measure_query_timings('old', 'new', 'SELECT 42')
        self.assertEqual(measured.runs, 10)
        self.assertEqual(timings.call_count, 4)

    def test_profiled_join_work_regression_reaches_runner(self):
        for time_format in ('query', 'latency', 'timing'):
            for old_build, new_build, cold_time, hot_time, regressed in (
                (10, 40, 1.1, 1.0, False),
                (10, 40, 0.9, 1.1, True),
                (40, 10, 1.1, 1.1, True),
            ):
                with self.subTest(time_format=time_format, old_build=old_build, hot_time=hot_time):
                    profiles = []
                    for tree, elapsed in (
                        (join('MARK', 20, scan(20), scan(old_build)), 1.0),
                        (join('SEMI', 5, scan(100), scan(new_build)), cold_time),
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
                            stack.enter_context(
                                patch.object(
                                    plan_cost_runner,
                                    'measure_query_timings',
                                    return_value=benchmark_measurement([1.0] * 10, [hot_time] * 10),
                                )
                            )
                            for name, value in (
                                ('DB_NAME', root / 'plan_cost.duckdb'),
                                ('PROFILE_FILENAME', profile_path),
                            ):
                                stack.enter_context(patch.object(plan_cost_runner, name, str(value)))
                            stack.enter_context(
                                patch('sys.argv', ['runner', '--old', 'old', '--new', 'new', '--dir', directory])
                            )
                            stack.enter_context(contextlib.redirect_stdout(output))
                            with self.assertRaises(SystemExit) as result:
                                plan_cost_runner.main()
                        self.assertEqual(result.exception.code, int(regressed))
                        self.assertEqual('REGRESSIONS DETECTED' in output.getvalue(), regressed)
                        self.assertNotIn('IMPROVEMENTS DETECTED', output.getvalue())

    def test_shared_fixture_isolates_reader_regressions(self):
        for scenario in ('image', 'reader', 'structural'):
            with self.subTest(scenario=scenario), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                (root / 'queries').mkdir()
                (root / 'queries/q.sql').write_text('SELECT 1;')
                database = root / 'plan_cost.duckdb'
                profile = root / 'profile.json'

                def init_db(cli, dbname, benchmark_dir, extensions):
                    Path(dbname).write_text(cli)

                def run_command(command, **kwargs):
                    cli, readonly, dbname = command[:3]
                    self.assertEqual(readonly, '--readonly')
                    self.assertEqual(dbname, str(database))
                    changed = Path(dbname).read_text() == 'new' or (scenario != 'image' and cli == 'new')
                    if '.timer on 6' in command:
                        self.assertNotEqual(scenario, 'structural')
                        seconds = 1.06 if changed else 1.0
                        runs = command.count('SELECT 1;') - 1
                        output = f'Run Time (s): real {seconds} user 0 sys 0\n' * runs
                    else:
                        rows, probe_rows, build_rows = (30, 100, 40 if scenario == 'structural' else 10)
                        tree = (
                            join('INNER', rows, scan(probe_rows), scan(build_rows))
                            if changed
                            else join('INNER', 20, scan(20), scan(10))
                        )
                        profile.write_text(json.dumps({'children': [tree], 'latency': 1.0}))
                        output = ''
                    return subprocess.CompletedProcess(command, 0, stdout=output, stderr='')

                with contextlib.ExitStack() as stack:
                    initialize = stack.enter_context(patch.object(plan_cost_runner, 'init_db', side_effect=init_db))
                    stack.enter_context(patch.object(plan_cost_runner, 'run_command', side_effect=run_command))
                    stack.enter_context(patch.object(plan_cost_runner, 'DB_NAME', str(database)))
                    stack.enter_context(patch.object(plan_cost_runner, 'PROFILE_FILENAME', str(profile)))
                    stack.enter_context(
                        patch('sys.argv', ['runner', '--old', 'old', '--new', 'new', '--dir', directory])
                    )
                    stack.enter_context(contextlib.redirect_stdout(io.StringIO()))
                    with self.assertRaises(SystemExit) as result:
                        plan_cost_runner.main()
                    initialize.assert_called_once_with('old', str(database), directory, [])
                self.assertEqual(result.exception.code, int(scenario != 'image'))


if __name__ == '__main__':
    unittest.main()

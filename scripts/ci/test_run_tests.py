#!/usr/bin/env python3
import os
import sys
import tempfile
import textwrap
import unittest
from unittest import mock
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.ci import run_tests


def create_temp_file(content: str, **format_vars: object) -> Path:
    rendered_content = textwrap.dedent(content).lstrip().format(**format_vars)
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf8", delete=False) as tmp_file:
        tmp_file.write(rendered_content)
        tmp_file.flush()
        return Path(tmp_file.name)


def start_runner(cli_args: list[str]):
    return run_tests.invoke(cli_args, cwd=REPO_ROOT)


class RunTestsScriptTest(unittest.TestCase):
    def test_reports_skipped_tests_summary(self):
        cases = [
            {
                "name": "plain",
                "stdout": """
All tests passed (5 skipped tests, 123 assertions in 4 test cases)

Skipped tests for the following reasons:
require longdouble: 2
require-env SOME_TOKEN: 3
mode skip flaky planner: 2
mode skip unsupported: 1
""",
                "expected_skip_count": 5,
                "expected_reasons": [
                    "require longdouble: 2",
                    "require-env SOME_TOKEN: 3",
                    "mode skip flaky planner: 2",
                    "mode skip unsupported: 1",
                ],
            },
            {
                "name": "ansi",
                "stdout": (
                    "\x1b[36mAll tests passed (14 skipped tests, 2659 assertions in 86 test cases)\x1b[0m\n\n"
                    "\x1b[1mSkipped tests for the following reasons:\x1b[0m\n"
                    "\x1b[33mrequire httpfs: 1\x1b[0m\n"
                    "\x1b[33mrequire icu: 2\x1b[0m\n"
                    "\x1b[33mrequire-env LOCAL_EXTENSION_REPO: 5\x1b[0m\n"
                    "\x1b[33mmode skip flaky parser: 2\x1b[0m\n"
                ),
                "expected_skip_count": 14,
                "expected_reasons": [
                    "require httpfs: 1",
                    "require icu: 2",
                    "require-env LOCAL_EXTENSION_REPO: 5",
                    "mode skip flaky parser: 2",
                ],
            },
        ]

        for case in cases:
            with self.subTest(case=case["name"]):
                test_list_path = create_temp_file("test/sql/a.test\n")
                try:
                    with mock.patch(
                        "scripts.ci.run_tests.run_batch",
                        return_value={
                            "failed": False,
                            "stdout": case["stdout"],
                            "stderr": "",
                            "message": None,
                            "peak_rss_bytes": 0,
                        },
                    ):
                        proc = start_runner(
                            [
                                "--workers",
                                "1",
                                "--batch-size",
                                "1",
                                "--test-list",
                                str(test_list_path),
                                "--test-command",
                                "echo fake-run {test_list}",
                                "unused-binary",
                            ]
                        )
                finally:
                    test_list_path.unlink(missing_ok=True)

                self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
                self.assertIn("ran tests: ", proc.stdout)
                self.assertIn(f", {case['expected_skip_count']} skipped in ", proc.stdout)
                self.assertIn("Skipped tests for the following reasons:", proc.stdout)
                for expected_reason in case["expected_reasons"]:
                    self.assertIn(expected_reason, proc.stdout)

    def test_aggregates_skipped_tests_from_multiple_batches(self):
        test_list_path = create_temp_file("test/sql/a.test\ntest/sql/b.test\n")
        batch_outputs = [
            {
                "failed": False,
                "stdout": """
All tests passed (2 skipped tests, 100 assertions in 1 test cases)

Skipped tests for the following reasons:
require windows: 1
require-env A: 1
mode skip flaky planner: 2
""",
                "stderr": "",
                "message": None,
                "peak_rss_bytes": 0,
            },
            {
                "failed": False,
                "stdout": """
All tests passed (3 skipped tests, 100 assertions in 1 test cases)

Skipped tests for the following reasons:
require-env A: 2
require-env B: 1
mode skip flaky planner: 1
mode skip unsupported: 1
""",
                "stderr": "",
                "message": None,
                "peak_rss_bytes": 0,
            },
        ]
        try:
            with mock.patch("scripts.ci.run_tests.run_batch", side_effect=batch_outputs):
                proc = start_runner(
                    [
                        "--workers",
                        "1",
                        "--batch-size",
                        "1",
                        "--test-list",
                        str(test_list_path),
                        "--test-command",
                        "echo fake-run {test_list}",
                        "unused-binary",
                    ]
                )
        finally:
            test_list_path.unlink(missing_ok=True)

        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn(", 5 skipped in ", proc.stdout)
        self.assertIn("require windows: 1", proc.stdout)
        self.assertIn("require-env A: 3", proc.stdout)
        self.assertIn("require-env B: 1", proc.stdout)
        self.assertIn("mode skip flaky planner: 3", proc.stdout)
        self.assertIn("mode skip unsupported: 1", proc.stdout)

    def test_does_not_double_count_summary_when_in_stdout_and_stderr(self):
        test_list_path = create_temp_file("test/sql/a.test\n")
        skip_summary = """
All tests passed (4 skipped tests, 100 assertions in 1 test cases)

Skipped tests for the following reasons:
require json: 3
require-env FOO: 1
"""
        try:
            with mock.patch(
                "scripts.ci.run_tests.run_batch",
                return_value={
                    "failed": False,
                    "stdout": skip_summary,
                    "stderr": skip_summary,
                    "message": None,
                    "peak_rss_bytes": 0,
                },
            ):
                proc = start_runner(
                    [
                        "--workers",
                        "1",
                        "--batch-size",
                        "1",
                        "--test-list",
                        str(test_list_path),
                        "--test-command",
                        "echo fake-run {test_list}",
                        "unused-binary",
                    ]
                )
        finally:
            test_list_path.unlink(missing_ok=True)

        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn(", 4 skipped in ", proc.stdout)
        self.assertIn("require json: 3", proc.stdout)
        self.assertIn("require-env FOO: 1", proc.stdout)

    def test_retry_only_counts_skips_from_successful_attempt(self):
        test_list_path = create_temp_file("test/sql/a.test\n")
        try:
            with mock.patch(
                "scripts.ci.run_tests.run_batch",
                side_effect=[
                    {
                        "failed": True,
                        "stdout": """
All tests passed (7 skipped tests, 100 assertions in 1 test cases)

Skipped tests for the following reasons:
require windows: 7
""",
                        "stderr": "",
                        "message": "first attempt failed",
                        "peak_rss_bytes": 0,
                    },
                    {
                        "failed": False,
                        "stdout": """
All tests passed (2 skipped tests, 100 assertions in 1 test cases)

Skipped tests for the following reasons:
require windows: 2
""",
                        "stderr": "",
                        "message": None,
                        "peak_rss_bytes": 0,
                    },
                ],
            ):
                proc = start_runner(
                    [
                        "--workers",
                        "1",
                        "--retry",
                        "1",
                        "--batch-size",
                        "1",
                        "--test-list",
                        str(test_list_path),
                        "--test-command",
                        "echo fake-run {test_list}",
                        "unused-binary",
                    ]
                )
        finally:
            test_list_path.unlink(missing_ok=True)

        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn(", 2 skipped in ", proc.stdout)
        summary_block = proc.stdout.rsplit("Skipped tests for the following reasons:", 1)[-1]
        self.assertIn("require windows: 2", summary_block)
        self.assertNotIn("require windows: 7", summary_block)

    def test_fail_require_skip_enabled_fails(self):
        test_list_path = create_temp_file("test/sql/a.test\n")
        helper_path = create_temp_file(
            """
            #!/bin/sh
            cat <<'EOF'
            Skipped tests for the following reasons:
            require mysql_scanner: 56
            require postgres_scanner: 2
            require spatial: 124
            EOF
            exit 0
            """
        )
        os.chmod(helper_path, 0o755)

        try:
            proc = start_runner(
                [
                    "--workers",
                    "1",
                    "--batch-size",
                    "1",
                    "--fail-require-skip",
                    "--test-list",
                    str(test_list_path),
                    "--test-command",
                    f"{helper_path} {{test_list}}",
                    "unused-binary",
                ]
            )
        finally:
            test_list_path.unlink(missing_ok=True)
            helper_path.unlink(missing_ok=True)

        self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
        self.assertIn("require mysql_scanner: 56", proc.stdout)
        self.assertIn("require postgres_scanner: 2", proc.stdout)
        self.assertIn("require spatial: 124", proc.stdout)

    def test_generate_list(self):
        listed_tests_path = create_temp_file(
            """
            name\tgroup
            test/sql/slow.test\t[.][slow]
            test/sql/fast.test\t[fast]
            """
        )

        list_helper_path = create_temp_file(
            """
            #!/bin/sh
            # run_tests.py calls: <helper> --list-tests <pattern>
            if [ "$1" != "--list-tests" ]; then
              printf '\\n[0/2] (0%%): test/sql/slow.test\\n'
              printf '[1/2] (50%%): test/sql/slow.test took 0.001s\\n'
              printf '[2/2] (100%%): test/sql/fast.test took 0.002s\\n'
              printf 'All tests passed (100 assertions in 2 test cases)\\n'
              exit 0
            fi
            cat "$2"
            exit 2
            """
        )

        os.chmod(list_helper_path, 0o755)

        try:
            proc = start_runner(
                [
                    "--workers",
                    "1",
                    "--batch-size",
                    "2",
                    "--track-runtime",
                    "0",
                    "--test-command",
                    "{binary} {test_list}",
                    str(list_helper_path),
                    str(listed_tests_path),
                ]
            )
        finally:
            listed_tests_path.unlink(missing_ok=True)
            list_helper_path.unlink(missing_ok=True)

        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn("generated test list using:", proc.stdout)
        self.assertIn("batch_size=2", proc.stdout)
        self.assertNotIn("found 2 tests", proc.stdout)
        self.assertNotIn("forces batch_size=1", proc.stdout)
        self.assertIn("warn: test/sql/slow.test took", proc.stdout)
        self.assertIn("warn: test/sql/fast.test took", proc.stdout)
        self.assertIn("ran tests: ", proc.stdout)

    def test_runs_with_echo_test_command(self):
        test_list_path = create_temp_file("test/sql/a.test\ntest/sql/b.test\n")

        try:
            proc = start_runner(
                [
                    "--workers",
                    "1",
                    "--batch-size",
                    "2",
                    "--test-list",
                    str(test_list_path),
                    "--test-command",
                    "echo fake-run {test_list}",
                    "unused-binary",
                ]
            )
        finally:
            test_list_path.unlink(missing_ok=True)

        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertNotIn("found 2 tests", proc.stdout)
        self.assertIn("ran tests: ", proc.stdout)
        self.assertNotIn("=== config run: default ===", proc.stdout)
        self.assertNotIn("all 1 config runs passed", proc.stdout)

    def test_changed_tests_flag_uses_second_list_file(self):
        base_test_list_path = create_temp_file(
            """
            test/sql/a.test
            test/sql/b.test
            """
        )
        changed_test_list_path = create_temp_file(
            """
            test/sql/b.test
            test/sql/c.test
            """
        )

        list_helper_path = create_temp_file(
            """
            #!/bin/sh
            if [ "$1" != "--list-tests" ]; then
              exit 2
            fi
            shift
            while [ "$#" -gt 0 ]; do
              if [ "$1" = "-f" ]; then
                shift
                cat "$1"
              fi
              shift
            done
            exit 0
            """
        )

        os.chmod(list_helper_path, 0o755)

        try:
            proc = start_runner(
                [
                    "--workers",
                    "1",
                    "--batch-size",
                    "2",
                    "--test-list",
                    str(base_test_list_path),
                    "--changed-tests",
                    str(changed_test_list_path),
                    "--test-command",
                    "echo fake-run {test_list}",
                    str(list_helper_path),
                ]
            )
        finally:
            base_test_list_path.unlink(missing_ok=True)
            changed_test_list_path.unlink(missing_ok=True)
            list_helper_path.unlink(missing_ok=True)

        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn(f"-f {base_test_list_path}", proc.stdout)
        self.assertIn(f"-f {changed_test_list_path}", proc.stdout)
        self.assertIn("added 1 tests from --changed-tests file to the smoke test run", proc.stdout)
        self.assertIn("ran tests: ", proc.stdout)

    def test_stabilize_tests_reruns_selected_tests_with_fast_and_slow_policy(self):
        test_list_path = create_temp_file(
            """
            name\tgroup
            test/sql/fast.test\t[fast]
            test/sql/slow.test\t[.][slow]
            """
        )
        run_calls = []

        def fake_run_tests(_config, batches, total_tests):
            run_calls.append({"batches": batches, "total_tests": total_tests})
            return run_tests.ConfigRunResult(
                returncode=0, passed_tests=total_tests, failed_tests=0, skipped_tests=0, elapsed_seconds=0.0
            )

        try:
            with mock.patch("scripts.ci.run_tests.run_tests", side_effect=fake_run_tests):
                proc = start_runner(
                    [
                        "--workers",
                        "1",
                        "--batch-size",
                        "10",
                        "--stabilize-tests",
                        "--test-list",
                        str(test_list_path),
                        "--test-command",
                        "echo fake-run {test_list}",
                        "unused-binary",
                    ]
                )
        finally:
            test_list_path.unlink(missing_ok=True)

        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(len(run_calls), 12)
        self.assertEqual(run_calls[0]["total_tests"], 2)
        fast_reruns = [
            call for call in run_calls[1:] if call["total_tests"] == 1 and call["batches"][0][0].endswith("fast.test")
        ]
        slow_reruns = [
            call for call in run_calls[1:] if call["total_tests"] == 1 and call["batches"][0][0].endswith("slow.test")
        ]
        self.assertEqual(len(fast_reruns), 9)
        self.assertEqual(len(slow_reruns), 2)

    def test_changed_tests_auto_stabilize_reruns_only_added_tests(self):
        base_test_list_path = create_temp_file(
            """
            test/sql/a.test
            """
        )
        changed_test_list_path = create_temp_file(
            """
            test/sql/a.test
            test/sql/b.test
            """
        )
        listed_tests_path = create_temp_file(
            """
            name\tgroup
            test/sql/a.test\t[fast]
            test/sql/b.test\t[fast]
            """
        )
        list_helper_path = create_temp_file(
            """
            #!/bin/sh
            if [ "$1" != "--list-tests" ]; then
              exit 2
            fi
            cat "{listed_tests_path}"
            exit 0
            """,
            listed_tests_path=listed_tests_path,
        )
        os.chmod(list_helper_path, 0o755)
        run_calls = []

        def fake_run_tests(_config, batches, total_tests):
            run_calls.append({"batches": batches, "total_tests": total_tests})
            return run_tests.ConfigRunResult(
                returncode=0, passed_tests=total_tests, failed_tests=0, skipped_tests=0, elapsed_seconds=0.0
            )

        try:
            with mock.patch("scripts.ci.run_tests.run_tests", side_effect=fake_run_tests):
                proc = start_runner(
                    [
                        "--workers",
                        "1",
                        "--batch-size",
                        "10",
                        "--test-list",
                        str(base_test_list_path),
                        "--changed-tests",
                        str(changed_test_list_path),
                        "--test-command",
                        "echo fake-run {test_list}",
                        str(list_helper_path),
                    ]
                )
        finally:
            base_test_list_path.unlink(missing_ok=True)
            changed_test_list_path.unlink(missing_ok=True)
            listed_tests_path.unlink(missing_ok=True)
            list_helper_path.unlink(missing_ok=True)

        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        rerun_calls = run_calls[1:]
        self.assertEqual(len(rerun_calls), 9)
        for call in rerun_calls:
            self.assertEqual(call["total_tests"], 1)
            self.assertTrue(call["batches"][0][0].endswith("test/sql/b.test"))

    def test_changed_tests_large_set_uses_fast_three_total_runs(self):
        base_test_list_path = create_temp_file("test/sql/base.test\n")
        changed_lines = ["test/sql/base.test"] + [f"test/sql/new_{idx}.test" for idx in range(501)]
        changed_test_list_path = create_temp_file("\n".join(changed_lines) + "\n")
        listed_rows = (
            ["name\tgroup"]
            + [f"test/sql/new_{idx}.test\t[fast]" for idx in range(501)]
            + ["test/sql/base.test\t[fast]"]
        )
        listed_tests_path = create_temp_file("\n".join(listed_rows) + "\n")
        list_helper_path = create_temp_file(
            """
            #!/bin/sh
            if [ "$1" != "--list-tests" ]; then
              exit 2
            fi
            cat "{listed_tests_path}"
            exit 0
            """,
            listed_tests_path=listed_tests_path,
        )
        os.chmod(list_helper_path, 0o755)
        run_calls = []

        def fake_run_tests(_config, batches, total_tests):
            run_calls.append({"batches": batches, "total_tests": total_tests})
            return run_tests.ConfigRunResult(
                returncode=0, passed_tests=total_tests, failed_tests=0, skipped_tests=0, elapsed_seconds=0.0
            )

        try:
            with mock.patch("scripts.ci.run_tests.run_tests", side_effect=fake_run_tests):
                proc = start_runner(
                    [
                        "--workers",
                        "1",
                        "--batch-size",
                        "1000",
                        "--test-list",
                        str(base_test_list_path),
                        "--changed-tests",
                        str(changed_test_list_path),
                        "--test-command",
                        "echo fake-run {test_list}",
                        str(list_helper_path),
                    ]
                )
        finally:
            base_test_list_path.unlink(missing_ok=True)
            changed_test_list_path.unlink(missing_ok=True)
            listed_tests_path.unlink(missing_ok=True)
            list_helper_path.unlink(missing_ok=True)

        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(len(run_calls), 3)
        self.assertEqual(run_calls[0]["total_tests"], 502)
        self.assertEqual(run_calls[1]["total_tests"], 501)
        self.assertEqual(run_calls[2]["total_tests"], 501)

    def test_stabilization_failure_fails_config_run(self):
        test_list_path = create_temp_file(
            """
            name\tgroup
            test/sql/fast.test\t[fast]
            """
        )
        run_results = [
            run_tests.ConfigRunResult(
                returncode=0, passed_tests=1, failed_tests=0, skipped_tests=0, elapsed_seconds=0.0
            ),
            run_tests.ConfigRunResult(
                returncode=1, passed_tests=0, failed_tests=1, skipped_tests=0, elapsed_seconds=0.0
            ),
        ]

        try:
            with mock.patch("scripts.ci.run_tests.run_tests", side_effect=run_results):
                proc = start_runner(
                    [
                        "--workers",
                        "1",
                        "--batch-size",
                        "10",
                        "--stabilize-tests",
                        "--test-list",
                        str(test_list_path),
                        "--test-command",
                        "echo fake-run {test_list}",
                        "unused-binary",
                    ]
                )
        finally:
            test_list_path.unlink(missing_ok=True)

        self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
        self.assertIn("error: stabilization rerun failure detected", proc.stdout)

    def test_retries_failed_fake_job(self):
        test_list_path = create_temp_file("test/sql/a.test\n")
        state_file_path = create_temp_file("")
        helper_path = create_temp_file(
            """
            #!/bin/sh
            if [ ! -f {state_file_path} ]; then
              touch {state_file_path}
              echo fake failure
              exit 1
            fi
            echo fake success
            exit 0
            """,
            state_file_path=state_file_path,
        )

        os.chmod(helper_path, 0o755)
        state_file_path.unlink(missing_ok=True)

        try:
            proc = start_runner(
                [
                    "--workers",
                    "1",
                    "--batch-size",
                    "1",
                    "--retry",
                    "1",
                    "--max-retries",
                    "4",
                    "--test-list",
                    str(test_list_path),
                    "--test-command",
                    f"{helper_path} {{test_list}}",
                    "unused-binary",
                ]
            )
        finally:
            test_list_path.unlink(missing_ok=True)
            state_file_path.unlink(missing_ok=True)
            helper_path.unlink(missing_ok=True)

        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn("retrying failed test batch 0 (attempt 1/1, retry 1/4)", proc.stdout)
        self.assertIn("fake failure", proc.stdout)
        self.assertIn("ran tests: ", proc.stdout)

    def test_retries_timed_out_sleep_job(self):
        test_list_path = create_temp_file("test/sql/a.test\n")
        batch_timeout = 0.3

        try:
            with mock.patch(
                "scripts.ci.run_tests.run_batch",
                side_effect=[
                    {
                        "failed": True,
                        "stdout": "",
                        "stderr": "",
                        "message": f"batch timed out after {batch_timeout} seconds",
                        "peak_rss_bytes": 0,
                    },
                    {
                        "failed": False,
                        "stdout": "fake success\n",
                        "stderr": "",
                        "message": None,
                        "peak_rss_bytes": 0,
                    },
                ],
            ):
                proc = start_runner(
                    [
                        "--workers",
                        "1",
                        "--retry",
                        "1",
                        "--batch-timeout",
                        str(batch_timeout),
                        "--test-list",
                        str(test_list_path),
                        "--test-command",
                        "echo fake-run {test_list}",
                        "unused-binary",
                    ]
                )
        finally:
            test_list_path.unlink(missing_ok=True)

        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn(f"batch timed out after {batch_timeout} seconds", proc.stdout)
        self.assertIn("retrying failed test", proc.stdout)
        self.assertIn("ran tests: ", proc.stdout)

    def test_multiple_test_configs_run_independently(self):
        listed_tests_path = create_temp_file(
            """
            name\tgroup
            test/sql/fast.test\t[fast]
            """
        )

        list_helper_path = create_temp_file(
            """
            #!/bin/sh
            if [ "$1" = "--test-config" ] && [ "$2" = "test/configs/a.json" ] && [ "$3" = "--list-tests" ]; then
              echo "name\tgroup"
              echo "test/sql/a.test\t[fast]"
              exit 0
            fi
            if [ "$1" = "--test-config" ] && [ "$2" = "test/configs/b.json" ] && [ "$3" = "--list-tests" ]; then
              echo "name\tgroup"
              echo "test/sql/b.test\t[fast]"
              exit 0
            fi
            exit 2
            """
        )
        os.chmod(list_helper_path, 0o755)

        try:
            proc = start_runner(
                [
                    "--workers",
                    "1",
                    "--batch-size",
                    "1",
                    "--test-command",
                    "echo fake-run {test_list}",
                    "--test-config",
                    "test/configs/a.json",
                    "--test-config",
                    "test/configs/b.json",
                    str(list_helper_path),
                    str(listed_tests_path),
                ]
            )
        finally:
            list_helper_path.unlink(missing_ok=True)
            listed_tests_path.unlink(missing_ok=True)

        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn("running 2 configs", proc.stdout)
        self.assertEqual(proc.stdout.count("ran tests: "), 2)
        self.assertIn("all 2 config runs passed", proc.stdout)

    def test_multiple_test_configs_aggregate_failure(self):
        failing_helper = create_temp_file(
            """
            #!/bin/sh
            if [ "$1" = "--test-config" ] && [ "$2" = "test/configs/empty.json" ] && [ "$3" = "--list-tests" ]; then
              echo "name\tgroup"
              exit 0
            fi
            if [ "$1" = "--test-config" ] && [ "$3" = "--list-tests" ]; then
              echo "name\tgroup"
              echo "test/sql/a.test\t[fast]"
              exit 0
            fi
            exit 2
            """
        )
        os.chmod(failing_helper, 0o755)
        try:
            proc = start_runner(
                [
                    "--workers",
                    "1",
                    "--batch-size",
                    "1",
                    "--test-command",
                    "echo fake-run {test_list}",
                    "--test-config",
                    "test/configs/pass.json",
                    "--test-config",
                    "test/configs/empty.json",
                    str(failing_helper),
                    "ignored-pattern",
                ]
            )
        finally:
            failing_helper.unlink(missing_ok=True)

        self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
        self.assertIn("error: no tests selected for config 'test/configs/empty.json'", proc.stdout)
        self.assertIn("error: 1 config runs failed: test/configs/empty.json", proc.stdout)

    def test_ci_groups_close_when_all_configs_pass(self):
        listed_tests_path = create_temp_file(
            """
            name\tgroup
            test/sql/fast.test\t[fast]
            """
        )
        list_helper_path = create_temp_file(
            """
            #!/bin/sh
            if [ "$1" = "--test-config" ] && [ "$3" = "--list-tests" ]; then
              echo "name\tgroup"
              echo "test/sql/a.test\t[fast]"
              exit 0
            fi
            exit 2
            """
        )
        os.chmod(list_helper_path, 0o755)
        try:
            with mock.patch.dict(os.environ, {"CI": "1"}, clear=False):
                proc = start_runner(
                    [
                        "--workers",
                        "1",
                        "--batch-size",
                        "1",
                        "--test-command",
                        "echo fake-run {test_list}",
                        "--test-config",
                        "test/configs/a.json",
                        "--test-config",
                        "test/configs/b.json",
                        str(list_helper_path),
                        str(listed_tests_path),
                    ]
                )
        finally:
            list_helper_path.unlink(missing_ok=True)
            listed_tests_path.unlink(missing_ok=True)

        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn("::group::test config: test/configs/a.json", proc.stdout)
        self.assertIn("::group::test config: test/configs/b.json", proc.stdout)
        self.assertNotIn("=== config run: test/configs/a.json ===", proc.stdout)
        self.assertNotIn("=== config run: test/configs/b.json ===", proc.stdout)
        self.assertEqual(proc.stdout.count("::endgroup::"), 2)
        self.assertGreaterEqual(proc.stdout.count("ran tests: "), 2)

    def test_ci_groups_stay_open_after_first_failed_config(self):
        failing_helper = create_temp_file(
            """
            #!/bin/sh
            if [ "$1" = "--test-config" ] && [ "$2" = "test/configs/fail.json" ] && [ "$3" = "--list-tests" ]; then
              exit 1
            fi
            if [ "$1" = "--test-config" ] && [ "$3" = "--list-tests" ]; then
              echo "name\tgroup"
              echo "test/sql/a.test\t[fast]"
              exit 0
            fi
            exit 2
            """
        )
        os.chmod(failing_helper, 0o755)
        try:
            with mock.patch.dict(os.environ, {"CI": "1"}, clear=False):
                proc = start_runner(
                    [
                        "--workers",
                        "1",
                        "--batch-size",
                        "1",
                        "--test-command",
                        "echo fake-run {test_list}",
                        "--test-config",
                        "test/configs/fail.json",
                        "--test-config",
                        "test/configs/pass.json",
                        str(failing_helper),
                        "ignored-pattern",
                    ]
                )
        finally:
            failing_helper.unlink(missing_ok=True)

        self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
        self.assertIn("::group::test config: test/configs/fail.json", proc.stdout)
        self.assertIn("::group::test config: test/configs/pass.json", proc.stdout)
        self.assertEqual(proc.stdout.count("::endgroup::"), 2)
        self.assertIn("❌ ran tests: ", proc.stdout)


if __name__ == "__main__":
    unittest.main()

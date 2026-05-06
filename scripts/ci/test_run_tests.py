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

mode skip flaky planner
mode skip unsupported

Skipped tests for the following reasons:
require longdouble: 2
require-env SOME_TOKEN: 3
""",
                "expected_skip_count": 5,
                "expected_reasons": [
                    "require longdouble: 2",
                    "require-env SOME_TOKEN: 3",
                    "mode skip flaky planner: 1",
                    "mode skip unsupported: 1",
                ],
            },
            {
                "name": "ansi",
                "stdout": (
                    "\x1b[36mAll tests passed (14 skipped tests, 2659 assertions in 86 test cases)\x1b[0m\n\n"
                    "\x1b[33mmode skip flaky parser\x1b[0m\n"
                    "\x1b[1mSkipped tests for the following reasons:\x1b[0m\n"
                    "\x1b[33mrequire httpfs: 1\x1b[0m\n"
                    "\x1b[33mrequire icu: 2\x1b[0m\n"
                    "\x1b[33mrequire-env LOCAL_EXTENSION_REPO: 5\x1b[0m\n"
                ),
                "expected_skip_count": 14,
                "expected_reasons": [
                    "require httpfs: 1",
                    "require icu: 2",
                    "require-env LOCAL_EXTENSION_REPO: 5",
                    "mode skip flaky parser: 1",
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
                self.assertIn("all tests passed in ", proc.stdout)
                self.assertIn(f"({case['expected_skip_count']} skipped tests)", proc.stdout)
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

mode skip flaky planner

Skipped tests for the following reasons:
require windows: 1
require-env A: 1
""",
                "stderr": "",
                "message": None,
                "peak_rss_bytes": 0,
            },
            {
                "failed": False,
                "stdout": """
All tests passed (3 skipped tests, 100 assertions in 1 test cases)

mode skip flaky planner
mode skip unsupported

Skipped tests for the following reasons:
require-env A: 2
require-env B: 1
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
        self.assertIn("(5 skipped tests)", proc.stdout)
        self.assertIn("require windows: 1", proc.stdout)
        self.assertIn("require-env A: 3", proc.stdout)
        self.assertIn("require-env B: 1", proc.stdout)
        self.assertIn("mode skip flaky planner: 2", proc.stdout)
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
        self.assertIn("(4 skipped tests)", proc.stdout)
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
        self.assertIn("(2 skipped tests)", proc.stdout)
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
              exit 2
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
                    "echo fake-run {test_list}",
                    str(list_helper_path),
                    str(listed_tests_path),
                ]
            )
        finally:
            listed_tests_path.unlink(missing_ok=True)
            list_helper_path.unlink(missing_ok=True)

        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn("generated test list using:", proc.stdout)
        self.assertIn("found 2 tests", proc.stdout)
        self.assertIn("test/sql/slow.test took", proc.stdout)
        self.assertIn("test/sql/fast.test took", proc.stdout)
        self.assertIn("all tests passed in ", proc.stdout)

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
        self.assertIn("found 2 tests", proc.stdout)
        self.assertIn("all tests passed in ", proc.stdout)

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
        self.assertIn("all tests passed in ", proc.stdout)

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
        self.assertIn("all tests passed in ", proc.stdout)

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
        self.assertIn("all tests passed in ", proc.stdout)


if __name__ == "__main__":
    unittest.main()

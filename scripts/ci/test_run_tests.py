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

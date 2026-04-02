#!/usr/bin/env python3
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
RUN_TESTS = REPO_ROOT / "scripts" / "ci" / "run_tests.py"


class RunTestsScriptTest(unittest.TestCase):
    def test_generate_list(self):
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf8", delete=False) as listed_tests:
            listed_tests.write("name\tgroup\n")
            listed_tests.write("test/sql/slow.test\t[.][slow]\n")
            listed_tests.write("test/sql/fast.test\t[fast]\n")
            listed_tests.flush()
            listed_tests_path = Path(listed_tests.name)

        with tempfile.NamedTemporaryFile(mode="w", encoding="utf8", delete=False) as list_helper:
            list_helper.write("#!/bin/sh\n")
            # run_tests.py calls: <helper> --list-tests <pattern>
            list_helper.write('if [ "$1" != "--list-tests" ]; then\n')
            list_helper.write("  exit 2\n")
            list_helper.write("fi\n")
            list_helper.write('cat "$2"\n')
            list_helper.write("exit 2\n")
            list_helper.flush()
            list_helper_path = Path(list_helper.name)

        os.chmod(list_helper_path, 0o755)

        try:
            proc = subprocess.run(
                [
                    sys.executable,
                    str(RUN_TESTS),
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
                ],
                cwd=REPO_ROOT,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
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
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf8", delete=False) as test_list:
            test_list.write("test/sql/a.test\n")
            test_list.write("test/sql/b.test\n")
            test_list.flush()
            test_list_path = Path(test_list.name)

        try:
            proc = subprocess.run(
                [
                    sys.executable,
                    str(RUN_TESTS),
                    "--workers",
                    "1",
                    "--batch-size",
                    "2",
                    "--test-list",
                    str(test_list_path),
                    "--test-command",
                    "echo fake-run {test_list}",
                    "unused-binary",
                ],
                cwd=REPO_ROOT,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        finally:
            test_list_path.unlink(missing_ok=True)

        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn("found 2 tests", proc.stdout)
        self.assertIn("all tests passed in ", proc.stdout)

    def test_retries_failed_fake_job(self):
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf8", delete=False) as test_list:
            test_list.write("test/sql/a.test\n")
            test_list.flush()
            test_list_path = Path(test_list.name)

        with tempfile.NamedTemporaryFile(mode="w", encoding="utf8", delete=False) as state_file:
            state_file_path = Path(state_file.name)

        with tempfile.NamedTemporaryFile(mode="w", encoding="utf8", delete=False) as helper:
            helper.write("#!/bin/sh\n")
            helper.write(f"if [ ! -f {state_file_path} ]; then\n")
            helper.write(f"  touch {state_file_path}\n")
            helper.write("  echo fake failure\n")
            helper.write("  exit 1\n")
            helper.write("fi\n")
            helper.write("echo fake success\n")
            helper.write("exit 0\n")
            helper.flush()
            helper_path = Path(helper.name)

        os.chmod(helper_path, 0o755)
        state_file_path.unlink(missing_ok=True)

        try:
            proc = subprocess.run(
                [
                    sys.executable,
                    str(RUN_TESTS),
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
                ],
                cwd=REPO_ROOT,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
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
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf8", delete=False) as test_list:
            test_list.write("test/sql/a.test\n")
            test_list.flush()
            test_list_path = Path(test_list.name)

        with tempfile.NamedTemporaryFile(mode="w", encoding="utf8", delete=False) as state_file:
            state_file_path = Path(state_file.name)

        batch_timeout = 0.3

        with tempfile.NamedTemporaryFile(mode="w", encoding="utf8", delete=False) as helper:
            helper.write("#!/bin/sh\n")
            helper.write(f"if [ ! -f {state_file_path} ]; then\n")
            helper.write(f"  touch {state_file_path}\n")
            helper.write(f"  sleep {batch_timeout + 0.1}\n")
            helper.write("fi\n")
            helper.write("echo fake success\n")
            helper.write("exit 0\n")
            helper.flush()
            helper_path = Path(helper.name)

        os.chmod(helper_path, 0o755)
        state_file_path.unlink(missing_ok=True)

        try:
            proc = subprocess.run(
                [
                    sys.executable,
                    str(RUN_TESTS),
                    "--retry",
                    "1",
                    "--batch-timeout",
                    str(batch_timeout),
                    "--test-list",
                    str(test_list_path),
                    "--test-command",
                    f"{helper_path} {{test_list}}",
                    "unused-binary",
                ],
                cwd=REPO_ROOT,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        finally:
            test_list_path.unlink(missing_ok=True)
            state_file_path.unlink(missing_ok=True)
            helper_path.unlink(missing_ok=True)

        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn(f"batch timed out after {batch_timeout} seconds", proc.stdout)
        self.assertIn("retrying failed test", proc.stdout)
        self.assertIn("all tests passed in ", proc.stdout)


if __name__ == "__main__":
    unittest.main()

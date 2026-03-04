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
        self.assertIn("all tests passed.", proc.stdout)

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
        self.assertIn("all tests passed.", proc.stdout)


if __name__ == "__main__":
    unittest.main()

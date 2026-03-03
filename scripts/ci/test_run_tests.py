#!/usr/bin/env python3
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


if __name__ == "__main__":
    unittest.main()

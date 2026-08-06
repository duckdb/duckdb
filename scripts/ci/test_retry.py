#!/usr/bin/env python3
import os
import subprocess
import sys
import tempfile
import textwrap
import time
import unittest
from pathlib import Path
from unittest import mock

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.ci import retry


class RetryScriptTest(unittest.TestCase):
    def test_parse_timeout(self):
        self.assertEqual(retry.parse_timeout("30"), 30)
        self.assertEqual(retry.parse_timeout("45s"), 45)
        self.assertEqual(retry.parse_timeout("2m"), 120)
        self.assertEqual(retry.parse_timeout("1.5h"), 5400)

    def test_parse_timeout_rejects_invalid_values(self):
        for value in ["", "0", "-1", "1d", "seconds"]:
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    retry.parse_timeout(value)

    def test_run_command_without_timeout_preserves_completed_status(self):
        completed = retry.run_command([sys.executable, "-c", "raise SystemExit(7)"], "unused", None)
        self.assertEqual(completed.returncode, 7)

    def test_run_command_without_timeout_succeeds(self):
        completed = retry.run_command([sys.executable, "-c", "pass"], "unused", None)
        self.assertEqual(completed.returncode, 0)

    def test_main_returns_124_after_timeout(self):
        args = mock.Mock(
            retries=0,
            timeout="1s",
            timeout_seconds=1,
            command=[sys.executable, "-c", "pass"],
        )
        timeout = subprocess.TimeoutExpired(args.command, 1)
        with (
            mock.patch.object(retry, "parse_args", return_value=args),
            mock.patch.object(retry, "run_command", side_effect=timeout),
            mock.patch("builtins.print"),
        ):
            self.assertEqual(retry.main(), 124)

    def test_run_command_timeout_terminates_process_tree_before_reraising(self):
        command = [sys.executable, "-c", "pass"]
        process = mock.Mock()
        timeout = subprocess.TimeoutExpired(command, 1)
        process.wait.side_effect = timeout
        with (
            mock.patch.object(retry.os, "name", "posix"),
            mock.patch.object(retry.subprocess, "Popen", return_value=process) as popen,
            mock.patch.object(retry, "terminate_process_tree") as terminate_process_tree,
        ):
            with self.assertRaises(subprocess.TimeoutExpired):
                retry.run_command(command, retry.format_command(command), 1)

        popen.assert_called_once_with(command, start_new_session=True)
        terminate_process_tree.assert_called_once_with(process)

    @unittest.skipIf(os.name == "nt", "POSIX process groups are not available on Windows")
    def test_process_group_cleanup_terminates_descendants_that_ignore_sigterm(self):
        import fcntl
        import signal

        child_code = textwrap.dedent(
            """
            import fcntl
            import signal
            import sys
            import time
            from pathlib import Path

            with open(sys.argv[1], "w") as lock_file:
                fcntl.flock(lock_file, fcntl.LOCK_EX)
                signal.signal(signal.SIGTERM, signal.SIG_IGN)
                Path(sys.argv[2]).touch()
                time.sleep(60)
            """
        )
        parent_code = textwrap.dedent(
            """
            import subprocess
            import sys
            import time

            subprocess.Popen([sys.executable, "-c", sys.argv[1], sys.argv[2], sys.argv[3]])
            time.sleep(60)
            """
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            lock_path = Path(temp_dir) / "held.lock"
            ready_path = Path(temp_dir) / "ready"
            command = [sys.executable, "-c", parent_code, child_code, str(lock_path), str(ready_path)]
            process = subprocess.Popen(command, start_new_session=True)
            try:
                deadline = time.monotonic() + 5
                while not ready_path.exists():
                    self.assertIsNone(process.poll(), "process group exited before acquiring the lock")
                    self.assertLess(time.monotonic(), deadline, "process group did not acquire the lock")
                    time.sleep(0.005)

                with (
                    mock.patch.object(retry, "TERMINATE_GRACE_SECONDS", 0.05),
                    mock.patch.object(retry, "PROCESS_GROUP_POLL_SECONDS", 0.005),
                ):
                    retry.terminate_posix_process_group(process)
            finally:
                if retry.process_group_exists(process.pid):
                    os.killpg(process.pid, signal.SIGKILL)
                process.wait()

            with lock_path.open("w") as lock_file:
                try:
                    fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    self.fail("timed-out descendant still holds the lock")

    def test_windows_timeout_uses_shell_and_new_process_group(self):
        process = mock.Mock()
        process.wait.return_value = 0
        with (
            mock.patch.object(retry.os, "name", "nt"),
            mock.patch.object(retry.subprocess, "CREATE_NEW_PROCESS_GROUP", 512, create=True),
            mock.patch.object(retry.subprocess, "Popen", return_value=process) as popen,
        ):
            completed = retry.run_command(["where", "cmake"], "where cmake", 1)

        self.assertEqual(completed.returncode, 0)
        popen.assert_called_once_with("where cmake", shell=True, creationflags=512)

    def test_windows_cleanup_terminates_process_tree(self):
        process = mock.Mock(pid=123)
        taskkill_result = subprocess.CompletedProcess([], 0)
        with mock.patch.object(retry.subprocess, "run", return_value=taskkill_result) as run:
            retry.terminate_windows_process_tree(process)

        run.assert_called_once_with(
            ["taskkill", "/PID", "123", "/T", "/F"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        process.wait.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()

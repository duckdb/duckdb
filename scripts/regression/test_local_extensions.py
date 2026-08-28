import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.plan_cost_runner import run_sql_file
from scripts.regression.local_extensions import extension_loading_args


class TestLocalExtensions(unittest.TestCase):
    def test_plan_cost_runner_can_run_directly(self):
        runner = Path(__file__).resolve().parents[1] / "plan_cost_runner.py"

        completed = subprocess.run(
            [sys.executable, str(runner), "--help"],
            capture_output=True,
            check=False,
            text=True,
        )

        self.assertEqual(completed.returncode, 0, completed.stderr)

    def test_storage_size_runner_can_run_directly(self):
        runner = Path(__file__).resolve().parents[1] / "regression_test_storage_size.py"

        with tempfile.TemporaryDirectory() as temp_directory:
            missing_executable = str(Path(temp_directory) / "duckdb")
            completed = subprocess.run(
                [sys.executable, str(runner), "--old", missing_executable, "--new", missing_executable],
                capture_output=True,
                check=False,
                text=True,
            )

        self.assertEqual(completed.returncode, 1, completed.stderr)
        self.assertIn("Failed to find old runner", completed.stdout)

    def test_uses_artifact_local_repository(self):
        with tempfile.TemporaryDirectory() as temp_directory:
            release_directory = Path(temp_directory) / "release's"
            executable = release_directory / "duckdb"
            executable.parent.mkdir(parents=True)
            executable.touch()
            (release_directory / "repository").mkdir()

            args = extension_loading_args(str(executable), ["tpch", "httpfs"])

            self.assertEqual(args[:2], ["-unsigned", "-cmd"])
            self.assertIn("release''s/repository'", args[2])
            self.assertIn("LOAD tpch", args[2])
            self.assertIn("LOAD httpfs", args[2])

    def test_loads_static_extension_without_repository(self):
        with tempfile.TemporaryDirectory() as temp_directory:
            executable = Path(temp_directory) / "duckdb"
            executable.touch()

            args = extension_loading_args(str(executable), ["tpch"])

            self.assertEqual(args, ["-unsigned", "-cmd", "LOAD tpch;"])

    def test_rejects_invalid_extension_name(self):
        with self.assertRaisesRegex(ValueError, "Invalid extension name"):
            extension_loading_args("duckdb", ["tpch; SELECT 42"])

    def test_plan_cost_load_file_uses_requested_extension(self):
        with tempfile.TemporaryDirectory() as temp_directory:
            release_directory = Path(temp_directory) / "release"
            executable = release_directory / "duckdb"
            executable.parent.mkdir(parents=True)
            executable.touch()
            (release_directory / "repository").mkdir()
            sql_file = Path(temp_directory) / "load.sql"
            sql_file.write_text("CALL dbgen(sf=1);", encoding="utf-8")
            completed = subprocess.CompletedProcess([], 0)

            with patch("scripts.plan_cost_runner.run_command", return_value=completed) as run:
                result = run_sql_file(str(executable), "database.db", str(sql_file), ["tpch"])

            self.assertEqual(result.returncode, 0)
            command = run.call_args.args[0]
            self.assertIn("LOAD tpch", command[command.index("-cmd") + 1])
            self.assertEqual(command[-1], "database.db")
            self.assertEqual(run.call_args.kwargs["stdin"].name, str(sql_file))


if __name__ == "__main__":
    unittest.main()

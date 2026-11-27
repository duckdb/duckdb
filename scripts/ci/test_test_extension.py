#!/usr/bin/env python3
"""Run with DUCKDB_TEST_BINARY pointing at a shell built with test extensions."""

import json
import ctypes
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

from scripts.ci import run_tests
from scripts.ci.test_run_tests import REPO_ROOT, protocol_config, write_runner_wrapper


class NativeResult(ctypes.Structure):
    _fields_ = [
        ("column_count", ctypes.c_uint64),
        ("row_count", ctypes.c_uint64),
        ("rows_changed", ctypes.c_uint64),
        ("columns", ctypes.c_void_p),
        ("error_message", ctypes.c_void_p),
        ("internal_data", ctypes.c_void_p),
    ]


class NativeTestConnection:
    def __init__(self, library):
        self.library = library
        self.database = ctypes.c_void_p()
        self.connection = ctypes.c_void_p()
        config = ctypes.c_void_p()
        assert library.duckdb_create_config(ctypes.byref(config)) == 0
        try:
            assert library.duckdb_set_config(config, b"allow_unsigned_extensions", b"true") == 0
            assert library.duckdb_set_config(config, b"threads", b"2") == 0
            error = ctypes.c_void_p()
            status = library.duckdb_open_ext(None, ctypes.byref(self.database), config, ctypes.byref(error))
            message = ctypes.string_at(error).decode() if error.value else ""
            library.duckdb_free(error)
            assert status == 0, message
        finally:
            library.duckdb_destroy_config(ctypes.byref(config))
        assert library.duckdb_connect(self.database, ctypes.byref(self.connection)) == 0

    def query(self, sql):
        result = NativeResult()
        try:
            status = self.library.duckdb_query(self.connection, sql.encode(), ctypes.byref(result))
            error = self.library.duckdb_result_error(ctypes.byref(result))
            if status:
                return error.decode() if error else "unknown query error"
            return None
        finally:
            self.library.duckdb_destroy_result(ctypes.byref(result))

    def close(self):
        self.library.duckdb_disconnect(ctypes.byref(self.connection))
        self.library.duckdb_close(ctypes.byref(self.database))


def verify_process_lifecycle(binary, directory):
    """Run in a child process with one core library and two database instances."""
    build = Path(binary).resolve().parent
    os.environ["DUCKDB_TEST_BUILD_DIRECTORY"] = str(build)
    os.environ["DUCKDB_TEST_RUNNER_EXTENSION"] = str(run_tests.resolve_test_extension_path(build).resolve())
    candidates = [
        build / "src/libduckdb.so",
        build / "src/libduckdb.dylib",
        build / "duckdb.dll",
        build / "src/duckdb.dll",
    ]
    library_path = next((path for path in candidates if path.is_file()), None)
    assert library_path, f"Cannot find libduckdb beside {binary}"
    library = ctypes.CDLL(str(library_path), mode=ctypes.RTLD_GLOBAL)
    pointer = ctypes.c_void_p
    signatures = {
        "duckdb_create_config": ([ctypes.POINTER(pointer)], ctypes.c_int),
        "duckdb_set_config": ([pointer, ctypes.c_char_p, ctypes.c_char_p], ctypes.c_int),
        "duckdb_destroy_config": ([ctypes.POINTER(pointer)], None),
        "duckdb_open_ext": ([ctypes.c_char_p, ctypes.POINTER(pointer), pointer, ctypes.POINTER(pointer)], ctypes.c_int),
        "duckdb_connect": ([pointer, ctypes.POINTER(pointer)], ctypes.c_int),
        "duckdb_disconnect": ([ctypes.POINTER(pointer)], None),
        "duckdb_close": ([ctypes.POINTER(pointer)], None),
        "duckdb_free": ([pointer], None),
        "duckdb_query": ([pointer, ctypes.c_char_p, ctypes.POINTER(NativeResult)], ctypes.c_int),
        "duckdb_result_error": ([ctypes.POINTER(NativeResult)], ctypes.c_char_p),
        "duckdb_destroy_result": ([ctypes.POINTER(NativeResult)], None),
    }
    for name, (arguments, result) in signatures.items():
        function = getattr(library, name)
        function.argtypes = arguments
        function.restype = result

    root = Path(directory).resolve()
    os.chdir(root)
    latch = root / "latch"
    os.environ["DUCKDB_TEST_RUNNER_LATCH"] = str(latch)
    original_directory = Path.cwd()
    home_variable = "USERPROFILE" if os.name == "nt" else "HOME"
    original_home = os.environ.get(home_variable)
    connections = []

    def run_sql(name, state_name):
        return (
            "SELECT * FROM run_tests("
            f"{run_tests.sql_string_list(['cpp:duckdb_tests:Runner regression ' + name])}, "
            f"extension_roots = {run_tests.sql_string_list([build / 'test/extension'])}, "
            f"test_flags = {run_tests.sql_string_list(['--temp-dir-root', str(root / 'temp'), '--temp-dir-destroy', 'always'])}, "
            f"state_database = {run_tests.sql_string(root / (state_name + '.duckdb'))});"
        )

    def assert_restored():
        assert Path.cwd() == original_directory, (Path.cwd(), original_directory)
        assert os.environ.get(home_variable) == original_home
        # os.environ caches Python's values, so also inspect the native environment.
        libc = ctypes.CDLL("ucrtbase.dll" if os.name == "nt" else None)
        libc.getenv.argtypes = [ctypes.c_char_p]
        libc.getenv.restype = ctypes.c_char_p
        actual = libc.getenv(home_variable.encode())
        assert (actual.decode() if actual else None) == original_home
        assert not (root / "temp").exists()

    try:
        for _ in range(2):
            connection = NativeTestConnection(library)
            connections.append(connection)
            load = run_tests.build_test_protocol_sql(binary, "SELECT 42;")
            load_error = connection.query(load)
            assert load_error is None, load_error
        first, second = connections
        failure_file = root / "failure.test"
        failure_file.write_text("statement ok\nSELECT nonexistent_column;\n")
        for index, connection in enumerate((first, second, first)):
            state_database = root / f"sql_failure_{index}.duckdb"
            query = (
                f"SELECT * FROM run_tests({run_tests.sql_string_list(['sql:' + failure_file.as_posix()])}, "
                f"test_roots={run_tests.sql_string_list([root])}, "
                f"test_flags={run_tests.sql_string_list(['--temp-dir-root', str(root / 'temp'), '--temp-dir-destroy', 'always'])}, "
                f"state_database={run_tests.sql_string(state_database)});"
            )
            assert connection.query(query) is None
            alias = f"failure_{index}"
            assert connection.query(f"ATTACH {run_tests.sql_string(state_database)} AS {alias};") is None
            error = connection.query(
                "SELECT CASE WHEN count(*) = 1 AND "
                "bool_and(status='failed' AND contains(diagnostics, 'Binder Error') "
                "AND contains(diagnostics, 'nonexistent_column')) "
                f"THEN true ELSE error('Missing failure diagnostics') END FROM {alias}.test_results;"
            )
            assert error is None, error
            assert_restored()
        # Discovery must leave each database free to choose flags for later calls.
        assert first.query("SELECT count(*) FROM scan_tests([], test_roots=[]);") is None
        invalid = second.query("SELECT * FROM run_tests([], test_flags=['--temp-dir-root']);")
        assert invalid and "expected an argument" in invalid, invalid
        assert_restored()
        for index, flags in enumerate(("--force-reload", "--single-threaded")):
            query = run_sql("passing", f"flags_{index}")
            query = query.replace("'--temp-dir-root'", f"'{flags}', '--temp-dir-root'")
            assert second.query(query) is None
            assert_restored()
        # Binding two statements must not make one statement inherit the other's flags.
        assert (
            first.query(
                "PREPARE runner_one AS "
                + run_sql("passing", "prepared_one").replace(
                    "'--temp-dir-root'", "'--force-storage', '--temp-dir-root'"
                )
            )
            is None
        )
        assert first.query("PREPARE runner_two AS " + run_sql("passing", "prepared_two")) is None
        assert first.query("EXECUTE runner_two") is None
        assert first.query("EXECUTE runner_one") is None
        assert_restored()
        results = []
        worker = threading.Thread(target=lambda: results.append(first.query(run_sql("blocking", "blocking"))))
        worker.start()
        try:
            deadline = time.monotonic() + 15
            while not Path(str(latch) + ".ready").exists():
                assert worker.is_alive(), results
                assert time.monotonic() < deadline, "blocking fixture did not signal readiness"
                time.sleep(0.01)
            error = second.query(run_sql("passing", "overlap"))
            assert error and "A test run is already active in this process" in error, error
        finally:
            Path(str(latch) + ".release").touch()
            worker.join(timeout=25)
        assert not worker.is_alive(), "blocking fixture did not finish"
        assert results == [None], results
        assert_restored()
        for index, name in enumerate(["passing", "passing", "nested invocation", "registry survives cache eviction"]):
            error = second.query(run_sql(name, f"sequential_{index}"))
            assert error is None, error
            assert_restored()
        error = first.query(run_sql("failure context", "failure"))
        assert error and "Catch exit code" in error, error
        assert_restored()
        error = second.query(run_sql("passing", "after_failure"))
        assert error is None, error
        assert_restored()
        error = first.query(run_sql("passing", "after_failure"))
        assert error and "already exists" in error, error
        assert_restored()
        error = second.query(run_sql("passing", "after_initialization_failure"))
        assert error is None, error
        assert_restored()
    finally:
        for connection in reversed(connections):
            connection.close()


@unittest.skipUnless(os.environ.get("DUCKDB_TEST_BINARY"), "requires DUCKDB_TEST_BINARY")
class TestExtensionIntegrationTest(unittest.TestCase):
    def setUp(self):
        self.binary = str(Path(os.environ["DUCKDB_TEST_BINARY"]).resolve())
        self.test_extension = run_tests.resolve_test_extension_path(Path(self.binary).parent).resolve()
        self.assertTrue(self.test_extension.is_file(), self.test_extension)
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.test_root = self.root / "tests"
        self.test_root.mkdir()
        self.temp_root = self.root / "temp"

    def create_test(self, name, body):
        path = self.test_root / name
        path.write_text(body)
        return "sql:" + path.as_posix()

    def query(self, sql, database=None):
        command = run_tests.test_protocol_command(self.binary)
        if database is not None:
            command.append(str(database))
        result = subprocess.run(command, input=sql, capture_output=True, text=True, timeout=60)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        return [json.loads(line) for line in result.stdout.splitlines() if line.startswith("{")]

    def run_selected(self, test_ids):
        flags = [
            "--temp-dir-root",
            str(self.temp_root),
            "--run-id",
            "integration",
            "--temp-dir-run-id",
            "on",
        ]
        sql = (
            "SELECT * FROM run_tests("
            f"{run_tests.sql_string_list(test_ids)}, "
            f"test_roots = {run_tests.sql_string_list([self.test_root])}, "
            f"test_flags = {run_tests.sql_string_list(flags)});"
        )
        rows = self.query(run_tests.build_test_protocol_sql(self.binary, sql))
        self.assertEqual(len(rows), 1, rows)
        return Path(rows[0]["state_database"])

    def test_default_state_database_survives_successful_cleanup(self):
        test_id = self.create_test("pass.test", "query I\nSELECT 42\n----\n42\n")
        database = self.run_selected([test_id])
        self.assertTrue(database.is_file(), database)
        self.assertFalse((self.temp_root / "integration").exists())
        rows = self.query("SELECT test_id, status FROM test_results;", database)
        self.assertEqual(rows, [{"test_id": test_id, "status": "passed"}])

    def test_repeated_sql_failures_preserve_diagnostics(self):
        failing = self.create_test("failure.test", "statement ok\nSELECT nonexistent_column;\n")
        passing = self.create_test("passing.test", "query I\nSELECT 42\n----\n42\n")
        statements = []
        for index, test_id in enumerate((failing, failing, passing, failing)):
            database = self.root / f"results_{index}.duckdb"
            statements.append(
                f"SELECT * FROM run_tests({run_tests.sql_string_list([test_id])}, "
                f"test_roots={run_tests.sql_string_list([self.test_root])}, "
                f"test_flags={run_tests.sql_string_list(['--temp-dir-root', str(self.temp_root)])}, "
                f"state_database={run_tests.sql_string(database)});"
            )
            statements.append(f"ATTACH {run_tests.sql_string(database)} AS result_{index};")
        statements.append(
            " UNION ALL ".join(
                f"SELECT {index} AS invocation, status, diagnostics FROM result_{index}.test_results"
                for index in range(4)
            )
            + " ORDER BY invocation;"
        )
        rows = self.query(run_tests.build_test_protocol_sql(self.binary, "\n".join(statements)))
        results = [row for row in rows if "invocation" in row]
        self.assertEqual(len(results), 4, rows)
        for index, row in enumerate(results):
            if index == 2:
                self.assertEqual(row["status"], "passed")
                self.assertNotIn("Binder Error", row["diagnostics"] or "")
            else:
                self.assertEqual(row["status"], "failed")
                self.assertIn("Binder Error", row["diagnostics"])
                self.assertIn("nonexistent_column", row["diagnostics"])

    def test_legacy_unittest_entrypoint_runs_tests_from_generated_extension_roots(self):
        self.create_test(
            "legacy_runner_external_root.test",
            "query I\nSELECT count(*) FROM read_text('CMakeLists.txt')\n----\n1\n",
        )
        build_dir = self.root / "build"
        build_dir.mkdir()
        duckdb_name = Path(self.binary).name
        (build_dir / duckdb_name).symlink_to(self.binary)
        wrapper = build_dir / "test" / ("unittest.py" if os.name == "nt" else "unittest")
        write_runner_wrapper(wrapper, [self.test_root], duckdb_name, legacy=True)
        extension_dir = build_dir / "extension" / "test"
        extension_dir.mkdir(parents=True)
        (extension_dir / "test.duckdb_extension").symlink_to(self.test_extension)
        command = [
            sys.executable,
            str(REPO_ROOT / "scripts" / "ci" / "run_tests.py"),
            str(wrapper),
            "--workers",
            "1",
            "--retry",
            "0",
            "--max-retries",
            "0",
            f"--test-flags=--temp-dir-root {self.temp_root}",
            "*legacy_runner_external_root.test",
        ]
        result = subprocess.run(command, cwd=self.root, capture_output=True, text=True, timeout=60)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("1 passed", result.stdout)

    def test_downloaded_test_extension_uses_the_current_build_directory(self):
        build_dir = self.root / "build" / "release"
        build_dir.mkdir(parents=True)
        binary = build_dir / Path(self.binary).name
        shutil.copy2(self.binary, binary)
        repository = build_dir / "repository" / "version" / "platform"
        repository.mkdir(parents=True)
        shutil.copy2(self.test_extension, repository / "test.duckdb_extension")
        self.create_test(
            "current_build_directory.test",
            f"query T\nSELECT '{{BUILD_DIRECTORY}}'\n----\n{build_dir.resolve()}\n",
        )
        result = subprocess.run(
            [
                sys.executable,
                str(REPO_ROOT / "scripts" / "ci" / "run_tests.py"),
                str(binary),
                "--test-root",
                str(self.test_root),
                "--workers",
                "1",
                "--retry",
                "0",
                f"--test-flags=--temp-dir-root {self.temp_root}",
                "*",
            ],
            cwd=self.root,
            capture_output=True,
            text=True,
            timeout=60,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("1 passed", result.stdout)

    def test_extension_ci_runs_only_configured_project_and_extension_tests(self):
        (self.root / "project.txt").write_text("project")
        project_test = self.root / "test" / "project.test"
        sqlite_test = self.root / "third_party" / "sqllogictest" / "test" / "sqlite.test"
        excluded_test = sqlite_test.parent / "evidence" / "in1.test"
        extension_root = self.root / "extension" / "test" / "sql"
        extension_test = extension_root / "extension.test"
        for path in [project_test, sqlite_test, excluded_test, extension_test]:
            path.parent.mkdir(parents=True, exist_ok=True)
        project_test.write_text("query I\nSELECT count(*) FROM read_text('project.txt')\n----\n1\n")
        sqlite_test.write_text("query I\nSELECT 42\n----\n42\n")
        excluded_test.write_text("statement error\nSELECT 42\n")
        (extension_root.parent.parent / "extension.txt").write_text("extension")
        extension_test.write_text("query I\nSELECT count(*) FROM read_text('extension.txt')\n----\n1\n")

        build_dir = self.root / "build"
        build_dir.mkdir()
        duckdb_name = Path(self.binary).name
        (build_dir / duckdb_name).symlink_to(self.binary)
        wrapper = build_dir / "test" / ("unittest.py" if os.name == "nt" else "unittest")
        write_runner_wrapper(wrapper, [extension_root], duckdb_name, test_root_directory=self.root)
        extension_dir = build_dir / "extension" / "test"
        extension_dir.mkdir(parents=True)
        (extension_dir / "test.duckdb_extension").symlink_to(self.test_extension)
        expected_names = {
            path.relative_to(self.root).as_posix() for path in [project_test, sqlite_test, extension_test]
        }
        commands = [
            [sys.executable, str(wrapper)],
            [sys.executable, str(REPO_ROOT / "scripts" / "ci" / "run_tests.py"), str(wrapper)],
        ]
        for command in commands:
            with self.subTest(command=command):
                result = subprocess.run(
                    [*command, "--list-tests", "*"], cwd=REPO_ROOT, capture_output=True, text=True, timeout=60
                )
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                listed_names = {line.split("\t")[0] for line in result.stdout.splitlines()[1:]}
                self.assertEqual(listed_names, expected_names, result.stdout)
                result = subprocess.run(
                    [
                        *command,
                        "--workers",
                        "1",
                        "--retry",
                        "0",
                        "--max-retries",
                        "0",
                        f"--test-flags=--temp-dir-root {self.temp_root}",
                        "*",
                    ],
                    cwd=REPO_ROOT,
                    capture_output=True,
                    text=True,
                    timeout=60,
                )
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                self.assertIn("3 passed, 0 skipped", result.stdout)

    def test_partial_skip_statistics_are_durable(self):
        test_id = self.create_test(
            "partial.test",
            "mode skip unsupported feature\n\nquery I\nSELECT 1\n----\n2\n\n"
            "mode unskip\n\nquery I\nSELECT 42\n----\n42\n",
        )
        database = self.run_selected([test_id])
        rows, error = run_tests.read_test_state(protocol_config(unittest_bin=self.binary), database, os.environ.copy())
        self.assertIsNone(error)
        self.assertEqual(
            [
                {
                    "status": row["status"],
                    "skip_mode": int(row["skip_mode"]),
                    "partial_skip_reasons": row["partial_skip_reasons"],
                }
                for row in rows
            ],
            [
                {
                    "status": "passed",
                    "skip_mode": 1,
                    "partial_skip_reasons": ["mode skip unsupported feature"],
                }
            ],
        )

    def test_reproducer_runs_with_an_installed_cli_from_another_directory(self):
        installed = self.root / "installed cli"
        installed.mkdir()
        cli = installed / ("duckdb.exe" if os.name == "nt" else "duckdb")
        shutil.copy2(self.binary, cli)
        work = self.root / "working directory"
        work.mkdir()
        bundle_root = self.root / "bundle roots"
        bundle_root.mkdir()
        test_id = self.create_test("reproduce.test", "query I\nSELECT 42\n----\n43\n")
        test_id = "sql:" + Path(test_id.removeprefix("sql:")).resolve().as_posix()
        command = [
            sys.executable,
            str(REPO_ROOT / "scripts/ci/run_tests.py"),
            str(cli),
            "--test-extension",
            str(self.test_extension),
            "--test-root",
            str(self.test_root),
            "--test-bundle-root",
            str(bundle_root),
            "--working-directory",
            str(work),
            "--workers",
            "1",
            "--retry",
            "0",
            "--max-retries",
            "0",
            test_id,
        ]
        environment = {**os.environ, "CI": ""}
        failed = subprocess.run(command, cwd=self.root, env=environment, capture_output=True, text=True, timeout=60)
        self.assertEqual(failed.returncode, 1, failed.stdout + failed.stderr)
        self.assertIn("reproduce all failed tests:\n", failed.stdout, failed.stdout + failed.stderr)
        reproduce = failed.stdout.split("reproduce all failed tests:\n", 1)[1].splitlines()[0]
        if os.name == "nt":
            arguments = reproduce
            for value in (str(cli), str(self.test_extension), str(self.test_root), str(bundle_root), str(work)):
                self.assertIn(value, reproduce)
        else:
            import shlex

            arguments = shlex.split(reproduce)
            parsed = run_tests.parse_args(arguments[2:])
            self.assertEqual(Path(parsed.unittest_bin), cli.resolve())
            self.assertEqual(parsed.test_extension, self.test_extension)
            self.assertEqual(parsed.test_root, [self.test_root.resolve()])
            self.assertEqual(parsed.test_bundle_root, [bundle_root.resolve()])
            self.assertEqual(parsed.working_directory, work.resolve())
        path = Path(test_id.removeprefix("sql:"))
        path.write_text("query I\nSELECT 42\n----\n42\n")
        other = self.root / "another directory"
        other.mkdir()
        passed = subprocess.run(arguments, cwd=other, env=environment, capture_output=True, text=True, timeout=60)
        self.assertEqual(passed.returncode, 0, passed.stdout + passed.stderr)

    def test_result_recovery_preserves_failure_messages_and_empty_lists(self):
        passed_id = self.create_test("passed.test", "query I\nSELECT 42\n----\n42\n")
        failed_id = self.create_test("failed.test", "query I\nSELECT 42\n----\n43\n")
        database = self.run_selected([passed_id, failed_id])
        rows, error = run_tests.read_test_state(protocol_config(unittest_bin=self.binary), database, os.environ.copy())
        self.assertIsNone(error)
        rows_by_id = {row["test_id"]: row for row in rows}
        self.assertEqual(rows_by_id[passed_id]["status"], "passed")
        self.assertEqual(rows_by_id[passed_id]["failures"], [])
        self.assertEqual(rows_by_id[failed_id]["status"], "failed")
        self.assertTrue(rows_by_id[failed_id]["failures"])
        for row in rows:
            self.assertEqual(row["partial_skip_reasons"], [])

    def test_bundle_discovery_does_not_load_fixture_extensions(self):
        extension_root = Path(self.binary).parent / "test" / "extension"
        for name in ["duckdb_tests.duckdb_extension", "loadable_extension_demo.duckdb_extension"]:
            self.assertTrue((extension_root / name).is_file(), name)
        sql = (
            "SELECT count(*) AS test_count FROM scan_tests("
            f"['*'], test_roots = [], extension_roots = {run_tests.sql_string_list([extension_root])});"
            "SELECT count(*) AS loaded_fixtures FROM duckdb_extensions() "
            "WHERE extension_name = 'loadable_extension_demo' AND loaded;"
        )
        rows = self.query(run_tests.build_test_protocol_sql(self.binary, sql))
        self.assertGreater(rows[0]["test_count"], 0)
        self.assertEqual(rows[1]["loaded_fixtures"], 0)

    def test_cpp_tests_have_separate_temporary_directories(self):
        names = ["Test file operations", "exclusive create fails when the file exists"]
        test_ids = ["cpp:duckdb_tests:" + name for name in names]
        flags = [
            "--temp-dir-root",
            str(self.temp_root),
            "--run-id",
            "integration",
            "--temp-dir-run-id",
            "on",
            "--temp-dir-destroy",
            "never",
        ]
        sql = (
            "SELECT * FROM run_tests("
            f"{run_tests.sql_string_list(test_ids)}, "
            f"extension_roots = {run_tests.sql_string_list([Path(self.binary).parent / 'test/extension'])}, "
            f"test_flags = {run_tests.sql_string_list(flags)});"
        )
        result = self.query(run_tests.build_test_protocol_sql(self.binary, sql))
        rows = self.query("SELECT status FROM test_results;", result[0]["state_database"])
        self.assertEqual(rows, [{"status": "passed"}, {"status": "passed"}])
        for name in names:
            leaf = "".join(character if character.isalnum() or character in "_-" else "_" for character in name)
            self.assertTrue((self.temp_root / "integration" / leaf).is_dir(), name)

    def cpp_test_id(self, name):
        return "cpp:duckdb_tests:Runner regression " + name

    def cpp_run_sql(self, test_ids, database):
        return run_tests.build_test_protocol_sql(
            self.binary,
            "SELECT * FROM run_tests("
            f"{run_tests.sql_string_list(test_ids)}, "
            f"extension_roots = {run_tests.sql_string_list([Path(self.binary).parent / 'test/extension'])}, "
            f"test_flags = {run_tests.sql_string_list(['--temp-dir-root', str(self.temp_root)])}, "
            f"state_database = {run_tests.sql_string(database)});",
        )

    def test_cpp_reporter_results_and_python_runner(self):
        environment = {**os.environ, "DUCKDB_TEST_RUNNER_REGRESSION": "1"}
        cases = [
            ("passing", "passed", 3),
            ("failure followed by skip", "failed", 2),
            ("failure context", "failed", 2),
            ("skip parquet", "skipped", 0),
        ]
        for name, status, count in cases:
            with self.subTest(name=name):
                test_id = self.cpp_test_id(name)
                database = self.root / (name.replace(" ", "_") + ".duckdb")
                result = subprocess.run(
                    [*run_tests.test_protocol_command(self.binary), "-bail"],
                    input=self.cpp_run_sql([test_id], database),
                    env=environment,
                    capture_output=True,
                    text=True,
                    timeout=60,
                )
                self.assertEqual(result.returncode != 0, status == "failed", result.stdout + result.stderr)
                rows, error = run_tests.read_test_state(
                    protocol_config(unittest_bin=self.binary), database, environment
                )
                self.assertIsNone(error)
                self.assertEqual(len(rows), 1)
                self.assertEqual(rows[0]["status"], status)
                self.assertEqual(int(rows[0]["assertion_count"]), count)
                if status == "failed":
                    self.assertTrue(rows[0]["failures"])
                if status == "skipped":
                    self.assertEqual(rows[0]["message"], "require parquet")
                if name == "failure context":
                    diagnostic = (
                        "\n".join(failure["message"] for failure in rows[0]["failures"]) + rows[0]["diagnostics"]
                    )
                    for context in ("input=314159", "captured := 271828", "stdout fixture", "stderr fixture"):
                        self.assertIn(context, diagnostic)
                runner = subprocess.run(
                    [
                        sys.executable,
                        str(REPO_ROOT / "scripts/ci/run_tests.py"),
                        self.binary,
                        "--workers",
                        "1",
                        "--retry",
                        "0",
                        "--max-retries",
                        "0",
                        f"--test-flags=--temp-dir-root {self.temp_root}",
                        test_id,
                    ],
                    env=environment,
                    capture_output=True,
                    text=True,
                    timeout=60,
                )
                self.assertEqual(runner.returncode != 0, status == "failed", runner.stdout + runner.stderr)
                if name == "failure context":
                    for context in ("input=314159", "captured := 271828", "stdout fixture", "stderr fixture"):
                        self.assertIn(context, runner.stdout + runner.stderr)

    def test_cpp_skip_reasons_belong_to_each_test(self):
        database = self.root / "skips.duckdb"
        ids = [self.cpp_test_id("skip parquet"), self.cpp_test_id("skip httpfs")]
        with mock.patch.dict(os.environ, {"DUCKDB_TEST_RUNNER_REGRESSION": "1"}):
            self.query(self.cpp_run_sql(ids, database))
        rows = self.query("SELECT test_id, status, message FROM test_results;", database)
        self.assertEqual(
            {row["test_id"]: (row["status"], row["message"]) for row in rows},
            {ids[0]: ("skipped", "require parquet"), ids[1]: ("skipped", "require httpfs")},
        )

    def test_cpp_session_error_preserves_completed_results(self):
        database = self.root / "session_error.duckdb"
        # Catch expands the wildcard, but its result cannot match this pending ID.
        ids = [self.cpp_test_id("passing"), self.cpp_test_id("skip *")]
        environment = {**os.environ, "DUCKDB_TEST_RUNNER_REGRESSION": "1"}
        result = subprocess.run(
            [*run_tests.test_protocol_command(self.binary), "-bail"],
            input=self.cpp_run_sql(ids, database),
            env=environment,
            capture_output=True,
            text=True,
            timeout=60,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Catch exit code", result.stderr)
        self.assertIn("did not match exactly one pending attempt", result.stderr)
        rows, error = run_tests.read_test_state(protocol_config(unittest_bin=self.binary), database, environment)
        self.assertIsNone(error)
        self.assertEqual({row["test_id"]: row["status"] for row in rows}, {ids[0]: "passed", ids[1]: "pending"})
        config = protocol_config(
            unittest_bin=self.binary,
            test_flags=f"--temp-dir-root {self.temp_root}",
        )
        with mock.patch.dict(os.environ, environment):
            batch = run_tests.run_batch(config, ids)
        self.assertTrue(batch["failed"], batch)
        self.assertNotEqual(batch["returncode"], 0)
        self.assertIn("Catch exit code", batch["stderr"])

    def test_runner_ignores_shell_startup_files(self):
        home = self.root / "home"
        home.mkdir()
        (home / ".duckdbrc").write_text(".print hello\n.open wrong_startup_database.duckdb\n")
        test_id = self.create_test("startup.test", "query I\nSELECT 42\n----\n42\n")
        environment = {**os.environ, "HOME": str(home), "USERPROFILE": str(home)}
        command = [
            sys.executable,
            str(REPO_ROOT / "scripts/ci/run_tests.py"),
            self.binary,
            "--test-root",
            str(self.test_root),
            "--workers",
            "1",
            "--retry",
            "0",
            f"--test-flags=--temp-dir-root {self.temp_root}",
            "*startup.test",
        ]
        for extra in (["--list-tests"], []):
            result = subprocess.run([*command, *extra], env=environment, capture_output=True, text=True, timeout=60)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertNotIn("hello", result.stdout + result.stderr)
        self.assertIn("1 passed", result.stdout)
        with mock.patch.dict(os.environ, environment):
            database = self.run_selected([test_id])
        rows, error = run_tests.read_test_state(protocol_config(unittest_bin=self.binary), database, environment)
        self.assertIsNone(error)
        self.assertEqual(rows[0]["status"], "passed")
        preflight = subprocess.run(
            [
                sys.executable,
                "-c",
                "import sys; from pathlib import Path; "
                "from scripts.ci.run_extension_autoload_tests import run_extension_tests; "
                "sys.exit(run_extension_tests(Path(sys.argv[1]), [], Path(sys.argv[2])))",
                self.binary,
                str(self.root),
            ],
            env=environment,
            capture_output=True,
            text=True,
            timeout=60,
        )
        self.assertEqual(preflight.returncode, 0, preflight.stdout + preflight.stderr)
        self.assertNotIn("hello", preflight.stdout + preflight.stderr)

    def test_process_execution_lifecycle(self):
        for home_present in (True, False):
            with self.subTest(home_present=home_present):
                root = self.root / ("home_present" if home_present else "home_unset")
                root.mkdir()
                environment = {**os.environ, "DUCKDB_TEST_RUNNER_REGRESSION": "1"}
                for name in ("HOME", "USERPROFILE"):
                    if home_present:
                        environment[name] = str(root)
                    else:
                        environment.pop(name, None)
                result = subprocess.run(
                    [
                        sys.executable,
                        "-c",
                        "import sys; from scripts.ci.test_test_extension import verify_process_lifecycle; "
                        "verify_process_lifecycle(sys.argv[1], sys.argv[2])",
                        self.binary,
                        str(root),
                    ],
                    env=environment,
                    capture_output=True,
                    text=True,
                    timeout=90,
                )
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_protocol_body_timeout_preserves_warning_policy(self):
        test_id = self.create_test(
            "slow.test",
            "query I\nSELECT count(*) FROM range(1000000000) a, range(1000000000) b\n----\n0\n",
        )
        config = protocol_config(
            unittest_bin=self.binary,
            test_flags=f"--temp-dir-root {self.temp_root}",
            batch_timeout_seconds=3,
            timeout_policy="warn",
        )
        config = run_tests.replace(
            config, invocation=run_tests.resolve_invocation(self.binary, test_roots=[self.test_root])
        )
        result = run_tests.run_batch(config, [test_id])
        self.assertTrue(result["timed_out"], result)
        self.assertTrue(run_tests.can_warn_timeout(config, [test_id], result), result)

    def test_batch_timeout_identifies_the_active_test_before_an_unstarted_skip(self):
        first = self.create_test("first.test", "query I\nSELECT 42\n----\n42\n")
        hanging = self.create_test(
            "hanging.test",
            "query I\nSELECT count(*) FROM range(1000000000) a, range(1000000000) b\n----\n0\n",
        )
        skipped = self.create_test("skipped.test", "require-env DUCKDB_CI_MISSING_TEST_VARIABLE\n")
        batch = [first, hanging, skipped]
        config = protocol_config(
            unittest_bin=self.binary,
            test_flags=f"--temp-dir-root {self.temp_root}",
            batch_timeout_seconds=3,
        )
        config = run_tests.replace(
            config, invocation=run_tests.resolve_invocation(self.binary, test_roots=[self.test_root])
        )
        result = run_tests.run_batch(config, batch)
        self.assertTrue(result["timed_out"], result)
        failure, lines, failed_names = run_tests.render_durable_failure(result, batch)
        self.assertEqual(failure.test_name, hanging)
        self.assertEqual(failure.reproduce_batch, [hanging])
        self.assertEqual(failed_names, [hanging])
        self.assertEqual(result["pending_tests"], [hanging, skipped])
        self.assertEqual(result["missing_tests"], [])
        self.assertIn("incomplete tests:", "\n".join(lines))
        with mock.patch.object(run_tests, "run_batch", return_value=result):
            summary = run_tests.run_tests(config, [batch], len(batch))
        self.assertEqual(summary.returncode, 1)
        self.assertEqual((summary.passed_tests, summary.failed_tests, summary.not_run_tests), (1, 1, 1))
        self.assertEqual(summary.failed_test_names, (hanging,))


if __name__ == "__main__":
    unittest.main()

#!/usr/bin/env python3

import contextlib
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
from unittest import mock

from scripts.ci import run_extension_autoload_tests as runner


@unittest.skipUnless(shutil.which("cmake"), "requires cmake")
class ExtensionTestConfigTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name).resolve()
        self.env = mock.patch.dict(os.environ, {}, clear=False)
        self.env.start()
        self.addCleanup(self.env.stop)
        os.environ.pop("BUILD_COMPLETE_EXTENSION_SET", None)

    def read(self, content, system_name="Linux"):
        config = self.root / "extensions.cmake"
        config.write_text(content, encoding="utf8")
        return runner.read_extension_configs(self.root, str(config), system_name)

    def test_guards_includes_and_first_declaration_precedence(self):
        (self.root / "included.cmake").write_text(
            '''
            duckdb_extension_load(disabled LOAD_TESTS GIT_URL ignored GIT_TAG ignored)
            duckdb_extension_load(first LOAD_TESTS GIT_URL ignored GIT_TAG ignored)
            duckdb_extension_load(active LOAD_TESTS GIT_URL https://example.com/active GIT_TAG active-pin)
            '''
        )
        entries = self.read(
            '''
            duckdb_extension_load(disabled DONT_BUILD)
            duckdb_extension_load(first GIT_URL first-url GIT_TAG first-pin)
            duckdb_extension_load(no_tests GIT_URL url GIT_TAG pin)
            if(WIN32)
                duckdb_extension_load(windows LOAD_TESTS GIT_URL url GIT_TAG pin)
            endif()
            if(CMAKE_SYSTEM_NAME STREQUAL "Linux" AND NOT MUSL_ENABLED)
                include("${CMAKE_CURRENT_LIST_DIR}/included.cmake")
            endif()
            # duckdb_extension_load(commented LOAD_TESTS)
            duckdb_extension_statically_link(core_functions)
            '''
        )
        self.assertEqual([entry["name"] for entry in entries], ["active"])
        self.assertEqual(entries[0]["git_tag"], "active-pin")

    def test_custom_paths_patches_and_submodule_lists(self):
        entries = self.read(
            r'''
            duckdb_extension_load(remote LOAD_TESTS APPLY_PATCHES
                GIT_URL "https://example.com/repo#fragment"
                GIT_TAG "exact-revision"
                SOURCE_DIR loader
                TEST_DIR "tests/custom sql"
                SUBMODULES "third_party/one;path with spaces")
            duckdb_extension_load(local LOAD_TESTS
                SOURCE_DIR "${PROJECT_SOURCE_DIR}/source with spaces"
                TEST_DIR "${PROJECT_SOURCE_DIR}/tests with spaces")
            '''
        )
        self.assertEqual(entries[0]["git_url"], "https://example.com/repo#fragment")
        self.assertEqual(entries[0]["git_tag"], "exact-revision")
        self.assertEqual(entries[0]["submodules"], ["third_party/one", "path with spaces"])
        self.assertTrue(entries[0]["apply_patches"])
        remote_root = self.root / "extension" / "external" / "remote" / "tests" / "custom sql"
        local_root = self.root / "tests with spaces"
        remote_root.mkdir(parents=True)
        local_root.mkdir()
        with mock.patch.object(
            runner.sync_out_of_tree_extensions, "sync_extension"
        ) as sync, contextlib.redirect_stdout(io.StringIO()):
            roots = runner.prepare_test_roots(self.root, entries)
        self.assertEqual(roots, [remote_root, local_root])
        sync.assert_called_once_with(entries[0], self.root / "extension" / "external", self.root)

    def test_missing_git_revision_fails_with_config_error(self):
        with self.assertRaises(subprocess.CalledProcessError) as error:
            self.read("duckdb_extension_load(remote LOAD_TESTS GIT_URL url)")
        self.assertIn("Git URL specified but no valid GIT_TAG was found for remote", error.exception.stderr)

    def test_skip_extensions_and_checkout_override_match_build_resolution(self):
        checkout = self.root / "overridden checkout"
        test_root = checkout / "custom tests"
        test_root.mkdir(parents=True)
        with mock.patch.dict(os.environ, {"SKIP_EXTENSIONS": "ignored", "DUCKDB_REMOTE_DIRECTORY": str(checkout)}):
            entries = self.read(
                '''
                duckdb_extension_load(ignored LOAD_TESTS)
                duckdb_extension_load(remote LOAD_TESTS GIT_URL url GIT_TAG pin
                    SOURCE_DIR loader INCLUDE_DIR headers TEST_DIR "custom tests")
                duckdb_extension_load(remote LOAD_TESTS SOURCE_DIR different)
            '''
            )
            self.assertEqual([entry["name"] for entry in entries], ["remote"])
            self.assertEqual(Path(entries[0]["source_path"]), checkout / "loader")
            self.assertEqual(Path(entries[0]["include_path"]), checkout / "headers")
            self.assertEqual(Path(entries[0]["test_path"]), test_root)
            with mock.patch.object(runner.sync_out_of_tree_extensions, "sync_extension") as sync:
                self.assertEqual(runner.prepare_test_roots(self.root, entries), [test_root])
                sync.assert_not_called()

    def test_absolute_git_test_root_is_preserved(self):
        test_root = self.root / "absolute tests"
        test_root.mkdir()
        entries = self.read(
            f'''
            duckdb_extension_load(remote LOAD_TESTS GIT_URL url GIT_TAG pin TEST_DIR "{test_root}")
        '''
        )
        with mock.patch.object(runner.sync_out_of_tree_extensions, "sync_extension"):
            self.assertEqual(runner.prepare_test_roots(self.root, entries), [test_root])

    def test_complete_extension_set_guard_honors_environment(self):
        config = '''
            if(BUILD_COMPLETE_EXTENSION_SET)
                duckdb_extension_load(complete LOAD_TESTS)
            endif()
        '''
        self.assertEqual([entry["name"] for entry in self.read(config)], ["complete"])
        os.environ["BUILD_COMPLETE_EXTENSION_SET"] = "0"
        self.assertEqual(self.read(config), [])

    def test_platform_guards_include_musl_and_mingw(self):
        config = self.root / "platform.cmake"
        config.write_text(
            '''
            if(NOT MUSL_ENABLED)
                duckdb_extension_load(glibc LOAD_TESTS)
            endif()
            if(MINGW)
                duckdb_extension_load(mingw LOAD_TESTS)
            endif()
            '''
        )
        self.assertEqual(runner.read_extension_configs(self.root, str(config), "Linux", "linux_amd64_musl"), [])
        self.assertEqual(
            [
                entry["name"]
                for entry in runner.read_extension_configs(self.root, str(config), "Windows", "windows_amd64_mingw")
            ],
            ["glibc", "mingw"],
        )

    def test_existing_build_sources_are_used_without_reapplying_patches(self):
        entries = self.read("duckdb_extension_load(remote LOAD_TESTS GIT_URL url GIT_TAG pin)")
        build = self.root / "build/release"
        test_root = build / "_deps/remote_extension_fc-src/test/sql"
        test_root.mkdir(parents=True)
        with mock.patch.object(runner.sync_out_of_tree_extensions, "sync_extension") as sync:
            self.assertEqual(runner.prepare_test_roots(self.root, entries, build), [test_root])
        sync.assert_not_called()

    def test_unity_catalog_config_uses_custom_root_for_synced_and_build_sources(self):
        config = runner.REPO_ROOT / ".github/config/extensions/unity_catalog.cmake"
        entries = runner.read_extension_configs(runner.REPO_ROOT, str(config), "Linux", "linux_amd64")
        self.assertEqual([entry["name"] for entry in entries], ["unity_catalog"])
        self.assertEqual(entries[0]["test_dir"], "test")
        build = self.root / "build/release"
        for use_build_source in (False, True):
            with self.subTest(use_build_source=use_build_source):
                source = (
                    build / "_deps/unity_catalog_extension_fc-src"
                    if use_build_source
                    else self.root / "extension/external/unity_catalog"
                )
                test_root = source / "test"
                (test_root / "functions").mkdir(parents=True)
                self.assertFalse((test_root / "sql").exists())
                with mock.patch.object(runner.sync_out_of_tree_extensions, "sync_extension") as sync:
                    with contextlib.redirect_stdout(io.StringIO()):
                        self.assertEqual(runner.prepare_test_roots(self.root, entries, build), [test_root])
                if use_build_source:
                    sync.assert_not_called()
                else:
                    sync.assert_called_once_with(entries[0], self.root / "extension/external", self.root)

    def test_empty_selection_and_missing_directory_fail(self):
        with self.assertRaisesRegex(RuntimeError, "No extension SQL test roots"):
            runner.prepare_test_roots(self.root, [])
        for test_dir in ("", 'TEST_DIR "tests/custom sql"'):
            with self.subTest(test_dir=test_dir):
                entries = self.read(f"duckdb_extension_load(missing LOAD_TESTS {test_dir})")
                with self.assertRaisesRegex(RuntimeError, "extension 'missing' does not exist"):
                    runner.prepare_test_roots(self.root, entries)


class ExtensionTestInvocationTest(unittest.TestCase):
    def test_empty_configs_check_extension_load_without_test_discovery(self):
        result = subprocess.CompletedProcess([], 0, stdout="", stderr="")
        with mock.patch.object(runner, "read_extension_configs", return_value=[]), contextlib.redirect_stdout(
            io.StringIO()
        ) as stdout:
            with mock.patch.object(runner.subprocess, "run", return_value=result) as run:
                self.assertEqual(runner.main(["--extension-configs", "empty.cmake", "--duckdb", "/build/duckdb"]), 0)
        self.assertEqual(run.call_count, 1)
        self.assertIn("-bail", run.call_args.args[0])
        self.assertIn("-no-init", run.call_args.args[0])
        self.assertIn("LOAD ", run.call_args.args[0][-1])
        self.assertIn("No extension SQL tests configured", stdout.getvalue())

    def test_empty_test_listing_fails(self):
        result = subprocess.CompletedProcess([], 0, stdout="name\tgroup\n", stderr="")
        with mock.patch.object(runner.subprocess, "run", return_value=result) as run:
            with self.assertRaisesRegex(RuntimeError, "No extension SQL tests selected"):
                runner.run_extension_tests(Path("/build/duckdb"), [Path("/extension/tests")], Path("/source"))
        self.assertEqual(run.call_count, 2)

    def test_load_failure_precedes_discovery(self):
        failure = subprocess.CalledProcessError(
            1, ["duckdb", "-c", "LOAD test"], stderr="Invalid access to memory location"
        )
        for roots in ([], [Path("/extension/tests")]):
            with self.subTest(roots=roots), mock.patch.object(runner.subprocess, "run", side_effect=failure) as run:
                with self.assertRaises(subprocess.CalledProcessError):
                    runner.run_extension_tests(Path("/build/duckdb"), roots, Path("/source"))
            self.assertEqual(run.call_count, 1)
            self.assertIn("-bail", run.call_args.args[0])

    def test_failed_discovery_preserves_cause(self):
        failure = subprocess.CalledProcessError(1, ["duckdb"], output="output", stderr="extension load failed")
        with mock.patch.object(runner, "read_extension_configs", side_effect=failure), contextlib.redirect_stderr(
            io.StringIO()
        ) as stderr:
            self.assertEqual(runner.main(["--extension-configs", "config.cmake"]), 1)
        self.assertIn("exited with status 1", stderr.getvalue())
        self.assertIn("extension load failed", stderr.getvalue())


@unittest.skipUnless(os.environ.get("DUCKDB_TEST_BINARY"), "requires DUCKDB_TEST_BINARY")
class ExtensionTestArtifactIntegrationTest(unittest.TestCase):
    @unittest.skipUnless(shutil.which("sh"), "requires a POSIX shell")
    def test_downloaded_artifacts_run_only_fast_extension_sql_tests_and_report_skips(self):
        binary = Path(os.environ["DUCKDB_TEST_BINARY"]).resolve()
        metadata = subprocess.run(
            [
                str(binary),
                "-no-agent",
                "-batch",
                "-bail",
                "-jsonlines",
                "-c",
                "SELECT CASE WHEN regexp_full_match(library_version, 'v[0-9]+[.][0-9]+[.][0-9]+') "
                "THEN library_version ELSE source_id END AS version, platform FROM pragma_version(), pragma_platform();",
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=60,
        )
        metadata = json.loads(metadata.stdout)
        with tempfile.TemporaryDirectory(prefix="duckdb-artifact-extension-tests-") as directory:
            root = Path(directory).resolve()
            build_dir = root / "build" / "release"
            repository = build_dir / "repository" / metadata["version"] / metadata["platform"]
            repository.mkdir(parents=True)
            shutil.copy2(binary, build_dir / binary.name)
            shutil.copy2(
                binary.parent / "extension" / "test" / "test.duckdb_extension",
                repository / "test.duckdb_extension",
            )
            shutil.copy2(binary.parent / "extension/json/json.duckdb_extension", repository / "json.duckdb_extension")
            test_root = root / "extension" / "fixture" / "test"
            for suite in ("databricks", "functions", "oss_local"):
                (test_root / suite).mkdir(parents=True)
            (test_root / "pass.test").write_text("query I\nSELECT 42\n----\n42\n")
            (test_root / "functions" / "present_extension.test").write_text(
                "require json\n\nquery I\nSELECT 42\n----\n42\n"
            )
            (test_root / "databricks" / "environment.test").write_text(
                "require-env DUCKDB_ARTIFACT_TEST_UNSET\n\nstatement error\nSELECT 'never reached';\n----\nmissing\n"
            )
            (test_root / "oss_local" / "missing_extension.test").write_text(
                "require delta\n\nstatement error\nSELECT 'never reached';\n----\nmissing\n"
            )
            for path in [
                root / "test" / "core.test",
                root / "third_party" / "sqllogictest" / "test" / "sqlite.test",
                test_root / "slow.test_slow",
            ]:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("query I\nSELECT 42\n----\n0\n")
            config = root / "extensions.cmake"
            config.write_text(
                'duckdb_extension_load(fixture LOAD_TESTS TEST_DIR "${PROJECT_SOURCE_DIR}/extension/fixture/test")'
            )
            extensions = runner.read_extension_configs(root, str(config), "Linux")
            with contextlib.redirect_stdout(io.StringIO()):
                roots = runner.prepare_test_roots(root, extensions)
            output = root / "output.txt"
            code = (
                "from pathlib import Path\n"
                "from scripts.ci.run_extension_autoload_tests import run_extension_tests\n"
                f"raise SystemExit(run_extension_tests(Path({str(build_dir / binary.name)!r}), "
                f"[Path({str(test_root)!r})], Path({str(root)!r})))\n"
            )
            env = os.environ.copy()
            env.pop("CI", None)
            env.pop("DUCKDB_ARTIFACT_TEST_UNSET", None)
            env["PYTHONPATH"] = str(runner.REPO_ROOT)
            env["LOCAL_EXTENSION_REPO"] = str(root / "host-workspace/build/release/repository")
            with output.open("w") as stream:
                result = subprocess.run(
                    [
                        shutil.which("sh"),
                        "-c",
                        'export LOCAL_EXTENSION_REPO="$PWD/build/release/repository"; exec "$@"',
                        "run-extension-tests",
                        runner.sys.executable,
                        "-c",
                        code,
                    ],
                    cwd=root,
                    env=env,
                    stdout=stream,
                    stderr=subprocess.STDOUT,
                    timeout=120,
                )
            text = output.read_text()
            self.assertEqual(result.returncode, 0, text)
            self.assertIn("Selected 4 extension SQL tests from 1 test roots", text)
            self.assertIn("2 skipped tests", text)
            self.assertIn("DUCKDB_ARTIFACT_TEST_UNSET", text)
            self.assertIn("require delta", text)
            self.assertNotIn("core.test", text)
            self.assertNotIn("sqlite.test", text)
            self.assertNotIn("slow.test_slow", text)
            self.assertNotIn("require json:", text)
            self.assertEqual(roots, [test_root])

    def test_installed_cli_can_install_load_and_discover_local_tests(self):
        binary = Path(os.environ["DUCKDB_TEST_BINARY"]).resolve()
        extension = binary.parent / "extension/test/test.duckdb_extension"
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            tests = root / "tests"
            tests.mkdir()
            fixture = tests / "example.test"
            fixture.write_text("query I\nSELECT 42\n----\n42\n")
            flags = runner.run_tests.sql_string_list(["--skip-compiled"])
            sql = (
                f"SET extension_directory={runner.run_tests.sql_string(root / 'installed')};"
                f"INSTALL {runner.run_tests.sql_string(extension)}; LOAD test;"
                "SELECT count(*) AS selected FROM scan_tests(['*'], "
                f"test_roots={runner.run_tests.sql_string_list([tests])}, test_flags={flags});"
                f"SELECT * FROM run_tests({runner.run_tests.sql_string_list(['sql:' + fixture.as_posix()])}, "
                f"test_roots={runner.run_tests.sql_string_list([tests])}, test_flags={flags});"
            )
            result = subprocess.run(
                [str(binary), "-unsigned", "-no-agent", "-batch", "-bail", "-jsonlines", "-c", sql],
                cwd=root,
                capture_output=True,
                text=True,
                timeout=60,
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            rows = [json.loads(line) for line in result.stdout.splitlines() if line.startswith("{")]
            self.assertEqual(rows[0], {"selected": 1})
            self.assertEqual(int(rows[-1]["test_count"]), 1)


if __name__ == "__main__":
    unittest.main()

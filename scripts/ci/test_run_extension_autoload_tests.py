#!/usr/bin/env python3

import contextlib
import io
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
        self.assertIn("No GIT_TAG specified for remote", error.exception.stderr)

    def test_complete_extension_set_guard_honors_environment(self):
        config = '''
            if(BUILD_COMPLETE_EXTENSION_SET)
                duckdb_extension_load(complete LOAD_TESTS)
            endif()
        '''
        self.assertEqual([entry["name"] for entry in self.read(config)], ["complete"])
        os.environ["BUILD_COMPLETE_EXTENSION_SET"] = "0"
        self.assertEqual(self.read(config), [])

    def test_empty_selection_and_missing_directory_fail(self):
        with self.assertRaisesRegex(RuntimeError, "No extension SQL test roots"):
            runner.prepare_test_roots(self.root, [])
        entries = self.read("duckdb_extension_load(missing LOAD_TESTS)")
        with self.assertRaisesRegex(RuntimeError, "extension 'missing' does not exist"):
            runner.prepare_test_roots(self.root, entries)


class ExtensionTestInvocationTest(unittest.TestCase):
    def test_empty_test_listing_fails(self):
        result = subprocess.CompletedProcess([], 0, stdout="name\tgroup\n", stderr="")
        with mock.patch.object(runner.subprocess, "run", return_value=result) as run:
            with self.assertRaisesRegex(RuntimeError, "No extension SQL tests selected"):
                runner.run_extension_tests(Path("/build/duckdb"), [Path("/extension/tests")], Path("/source"))
        self.assertEqual(run.call_count, 1)

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
    def test_downloaded_artifacts_run_only_fast_extension_sql_tests_and_report_skips(self):
        binary = Path(os.environ["DUCKDB_TEST_BINARY"]).resolve()
        with tempfile.TemporaryDirectory(prefix="duckdb-artifact-extension-tests-") as directory:
            root = Path(directory).resolve()
            build_dir = root / "build" / "release"
            repository = build_dir / "repository" / "version" / "platform"
            repository.mkdir(parents=True)
            shutil.copy2(binary, build_dir / binary.name)
            shutil.copy2(
                binary.parent / "extension" / "test" / "test.duckdb_extension",
                repository / "test.duckdb_extension",
            )
            test_root = root / "extension" / "fixture" / "test" / "sql"
            test_root.mkdir(parents=True)
            (test_root / "pass.test").write_text("query I\nSELECT 42\n----\n42\n")
            (test_root / "environment.test").write_text(
                "require-env DUCKDB_ARTIFACT_TEST_UNSET\n\nstatement error\nSELECT 'never reached';\n----\nmissing\n"
            )
            (test_root / "missing_extension.test").write_text(
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
            config.write_text("duckdb_extension_load(fixture LOAD_TESTS)")
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
            env["LOCAL_EXTENSION_REPO"] = str(build_dir / "repository")
            with output.open("w") as stream:
                result = subprocess.run(
                    [runner.sys.executable, "-c", code],
                    cwd=root,
                    env=env,
                    stdout=stream,
                    stderr=subprocess.STDOUT,
                    timeout=120,
                )
            text = output.read_text()
            self.assertEqual(result.returncode, 0, text)
            self.assertIn("Selected 3 extension SQL tests from 1 test roots", text)
            self.assertIn("skipped tests", text)
            self.assertIn("DUCKDB_ARTIFACT_TEST_UNSET", text)
            self.assertIn("require delta", text)
            self.assertNotIn("core.test", text)
            self.assertNotIn("sqlite.test", text)
            self.assertNotIn("slow.test_slow", text)
            self.assertEqual(roots, [test_root])


if __name__ == "__main__":
    unittest.main()

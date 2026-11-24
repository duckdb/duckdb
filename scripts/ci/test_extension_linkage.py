#!/usr/bin/env python3
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


REPO_ROOT = Path(__file__).resolve().parents[2]


@unittest.skipUnless(shutil.which("cmake") and shutil.which("ninja"), "requires CMake and Ninja")
class ExtensionLinkageTest(unittest.TestCase):
    def test_extensions_only_has_no_shell_or_test_runner_targets(self):
        with tempfile.TemporaryDirectory() as directory:
            build = Path(directory)
            archives = build / "prebuilt"
            archives.mkdir()
            names = ["core_functions", "icu", "json", "parquet", "autocomplete", "tpcds", "tpch"]
            core_archive = archives / ("duckdb_static.lib" if sys.platform == "win32" else "libduckdb_static.a")
            core_archive.touch()
            for name in names:
                (archives / (f"{name}_extension.lib" if sys.platform == "win32" else f"lib{name}_extension.a")).touch()
            config = build / "extensions.cmake"
            config.write_text(
                'set(DUCKDB_MAIN_EXTENSION_CONFIG_TYPE "IN_TREE")\n'
                f'include("{REPO_ROOT.as_posix()}/.github/config/core.cmake")\n'
            )
            query = build / ".cmake/api/v1/query"
            query.mkdir(parents=True)
            (query / "codemodel-v2").touch()
            result = subprocess.run(
                [
                    "cmake",
                    "-S",
                    str(REPO_ROOT),
                    "-B",
                    str(build),
                    "-G",
                    "Ninja",
                    "-DCMAKE_BUILD_TYPE=Release",
                    "-DBUILD_EXTENSIONS_ONLY=ON",
                    f"-DPREBUILT_BINARY={core_archive}",
                    f"-DPREBUILT_EXTENSIONS={';'.join(names)}",
                    f"-DDUCKDB_EXTENSION_CONFIGS={config}",
                ],
                capture_output=True,
                text=True,
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            reply = build / ".cmake/api/v1/reply"
            index = json.loads(next(reply.glob("index-*.json")).read_text())
            model = json.loads((reply / index["reply"]["codemodel-v2"]["jsonFile"]).read_text())
            targets = {target["name"] for target in model["configurations"][0]["targets"]}
            self.assertIn("tpch_loadable_extension", targets)
            self.assertFalse(
                targets.intersection(
                    {
                        "shell",
                        "test_extension",
                        "test_loadable_extension",
                        "test_helpers",
                        "test_sqlite_runner",
                        "duckdb_tests_loadable_extension",
                    }
                ),
                targets,
            )

    def check_linkage(
        self,
        static_build,
        wasm_source_build=False,
        prebuilt=False,
        sanitized=False,
        build_type="RelWithDebInfo",
        force_assert=None,
        force_debug=False,
        thread_sanitizer=False,
    ):
        if force_assert is None:
            force_assert = sanitized
        shared_shell = (
            sanitized
            and not prebuilt
            and not thread_sanitizer
            and sys.platform != "win32"
            and (force_assert or force_debug or build_type == "Debug")
        )
        with tempfile.TemporaryDirectory() as directory:
            build = Path(directory)
            query = build / ".cmake/api/v1/query"
            query.mkdir(parents=True)
            (query / "codemodel-v2").touch()
            extra_flags = []
            if prebuilt:
                archives = build / "prebuilt"
                archives.mkdir()
                for name in ("duckdb_static", "duckdb_shell", "test_extension", "tpch_extension"):
                    filename = f"{name}.lib" if sys.platform == "win32" else f"lib{name}.a"
                    (archives / filename).touch()
                core_archive = "duckdb_static.lib" if sys.platform == "win32" else "libduckdb_static.a"
                extra_flags = [
                    f"-DPREBUILT_BINARY={archives / core_archive}",
                    "-DPREBUILT_EXTENSIONS=test;tpch",
                ]
            result = subprocess.run(
                [
                    "cmake",
                    "-S",
                    str(REPO_ROOT),
                    "-B",
                    str(build),
                    "-G",
                    "Ninja",
                    f"-DCMAKE_BUILD_TYPE={build_type}",
                    "-DBUILD_EXTENSIONS=tpch",
                    "-DSTATICALLY_LINK_EXTENSIONS=core_functions;tpch;test",
                    f"-DEXTENSION_STATIC_BUILD={int(static_build)}",
                    f"-DWASM_LOADABLE_EXTENSIONS={int(wasm_source_build)}",
                    f"-DENABLE_SANITIZER={int(sanitized)}",
                    f"-DFORCE_ASSERT={int(force_assert)}",
                    f"-DFORCE_DEBUG={int(force_debug)}",
                    f"-DENABLE_THREAD_SANITIZER={int(thread_sanitizer)}",
                    "-DENABLE_UBSAN=OFF",
                    *extra_flags,
                ],
                capture_output=True,
                text=True,
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            reply = build / ".cmake/api/v1/reply"
            index = json.loads(next(reply.glob("index-*.json")).read_text())
            model = json.loads((reply / index["reply"]["codemodel-v2"]["jsonFile"]).read_text())
            targets = {
                target["name"]: json.loads((reply / target["jsonFile"]).read_text())
                for target in model["configurations"][0]["targets"]
            }

            if wasm_source_build:
                for name in ("test_extension", "test_loadable_extension"):
                    target = targets[name]
                    source = next(
                        source for source in target["sources"] if Path(source["path"]).name == "test_extension.cpp"
                    )
                    includes = target["compileGroups"][source["compileGroupIndex"]]["includes"]
                    for header in ("duckdb_test/test_bundle.hpp", "sqllogic_test_executor.hpp"):
                        self.assertTrue(
                            any((Path(include["path"]) / header).is_file() for include in includes),
                            f"{name} cannot resolve {header}",
                        )
                return

            def libraries(name):
                return " ".join(
                    fragment["fragment"]
                    for fragment in targets[name]["link"]["commandFragments"]
                    if fragment["role"] == "libraries"
                )

            tpch_libraries = libraries("tpch_loadable_extension")
            self.assertIn("tpch_extension", tpch_libraries)
            self.assertEqual("duckdb_static" in tpch_libraries, static_build, tpch_libraries)
            test_libraries = libraries("test_loadable_extension")
            self.assertEqual("duckdb_static" in test_libraries, static_build, test_libraries)
            for extension in ("core_functions_extension", "tpch_extension"):
                self.assertEqual(extension in test_libraries, static_build, test_libraries)
            test_archive = targets["test_extension"]["artifacts"][0]["path"].replace("\\", "/")
            self.assertEqual(test_libraries.replace("\\", "/").count(test_archive), 1, test_libraries)
            for name in ("test_loadable_extension", "duckdb_tests_loadable_extension"):
                linked = libraries(name)
                if sys.platform == "win32":
                    self.assertIn("duckdb_static", linked, f"{name}: {linked}")
                    for extension in ("core_functions_extension", "tpch_extension"):
                        self.assertIn(extension, linked, f"{name}: {linked}")
                    self.assertNotIn("tools/shell/duckdb.lib", linked.replace("\\", "/"), f"{name}: {linked}")
                elif name == "duckdb_tests_loadable_extension":
                    for library in ("duckdb_static", "core_functions_extension", "parquet_extension"):
                        self.assertNotIn(library, linked, f"{name}: {linked}")
            target_names = {target["id"]: name for name, target in targets.items()}

            def dependencies(name):
                return {target_names[dependency["id"]] for dependency in targets[name].get("dependencies", [])}

            if shared_shell:
                linked = libraries("shell").replace("\\", "/")
                shared_library = targets["duckdb"]["artifacts"][0]["path"].replace("\\", "/")
                self.assertIn(shared_library, linked)
                for archive in ("duckdb_static", "core_functions_extension", "tpch_extension", "test_extension"):
                    self.assertNotIn(archive, linked)

            self.assertIn("test_loadable_extension", dependencies("duckdb_local_extension_repo"))
            self.assertIn("tpch_loadable_extension", dependencies("duckdb_local_extension_repo"))
            if prebuilt:
                self.assertEqual(targets["duckdb_re2"]["type"], "STATIC_LIBRARY")
                self.assertIn("duckdb_re2", dependencies("test_loadable_extension"))
                self.assertEqual(
                    {Path(source["path"]).name for source in targets["test_extension"]["sources"]},
                    {"prebuilt_extension.cpp"},
                )
                self.assertNotIn("test_sqlite_runner", dependencies("test_extension"))
                self.assertNotIn("test_sqlite_runner", dependencies("test_loadable_extension"))
                test_prebuilt_archive = (
                    (archives / ("test_extension.lib" if sys.platform == "win32" else "libtest_extension.a"))
                    .relative_to(build)
                    .as_posix()
                )
                for name in ("shell", "test_loadable_extension"):
                    linked = libraries(name).replace("\\", "/")
                    self.assertLess(linked.index(test_prebuilt_archive), linked.index("test_helpers"), linked)
                    if (
                        name == "shell"
                        or (name == "test_loadable_extension" and static_build)
                        or sys.platform == "win32"
                    ):
                        self.assertLess(linked.index("test_helpers"), linked.rindex("duckdb_static"), linked)
            if sanitized and not thread_sanitizer and (force_assert or force_debug or build_type == "Debug"):
                for name in ("test_extension", "shell"):
                    compile_flags = " ".join(
                        fragment["fragment"]
                        for group in targets[name]["compileGroups"]
                        for fragment in group["compileCommandFragments"]
                    )
                    self.assertIn("-fsanitize=address", compile_flags, name)
            # Static consumers still need the core archive after their extension archives.
            for name, extension in (("shell", "tpch"), ("static_link_smoke", "parquet")):
                if name == "shell" and shared_shell:
                    continue
                linked = libraries(name)
                self.assertIn("duckdb_static", linked)
                self.assertLess(linked.index(f"{extension}_extension"), linked.rindex("duckdb_static"))

    @unittest.skipIf(sys.platform == "win32", "Windows requires EXTENSION_STATIC_BUILD")
    def test_dynamic_loadable_uses_host_duckdb(self):
        self.check_linkage(False)

    def test_static_loadable_includes_duckdb(self):
        self.check_linkage(True)

    def test_wasm_source_build_has_test_runner_headers(self):
        self.check_linkage(True, wasm_source_build=True)

    def test_prebuilt_extensions_are_packaged_without_recompiling_runner(self):
        self.check_linkage(True, prebuilt=True)

    @unittest.skipIf(sys.platform == "win32", "Windows requires EXTENSION_STATIC_BUILD")
    def test_prebuilt_dynamic_loadable_uses_host_duckdb(self):
        self.check_linkage(False, prebuilt=True)

    @unittest.skipIf(sys.platform == "win32", "Windows requires EXTENSION_STATIC_BUILD")
    def test_sanitized_loadable_uses_host_duckdb(self):
        self.check_linkage(False, sanitized=True)

    @unittest.skipIf(sys.platform == "win32", "Windows uses a static shell")
    def test_debug_sanitizer_shell_uses_shared_duckdb(self):
        self.check_linkage(False, sanitized=True, build_type="Debug", force_assert=False)

    @unittest.skipIf(sys.platform == "win32", "Windows uses a static shell")
    def test_release_sanitizer_option_keeps_static_shell(self):
        self.check_linkage(False, sanitized=True, build_type="Release", force_assert=False)

    @unittest.skipIf(sys.platform == "win32", "Windows uses a static shell")
    def test_forced_debug_sanitizer_shell_uses_shared_duckdb(self):
        self.check_linkage(False, sanitized=True, build_type="Release", force_assert=False, force_debug=True)

    @unittest.skipIf(sys.platform == "win32", "Windows uses a static shell")
    def test_thread_sanitizer_keeps_static_shell(self):
        self.check_linkage(False, sanitized=True, thread_sanitizer=True)


if __name__ == "__main__":
    unittest.main()

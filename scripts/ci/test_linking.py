"""Test Windows static export removal and self-contained extension linkage."""

import importlib.util
from collections import Counter
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts/strip_coff_exports.py"
SPEC = importlib.util.spec_from_file_location("strip_coff_exports", SCRIPT)
EXPORTS = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(EXPORTS)
LLVM_TOOLS = ["clang-cl", "llvm-lib", "lld-link", "llvm-readobj", "llvm-objcopy", "llvm-ar"]
CMAKE_TOOLS = ["cmake", "ninja", *LLVM_TOOLS]


def non_export_directives(archive):
    result = subprocess.run(["llvm-readobj", "--coff-directives", str(archive)], check=True, capture_output=True)
    return Counter(
        token
        for line in result.stdout.splitlines()
        if line.startswith(b"Directive(s):")
        for token in EXPORTS.filter_directives(line.removeprefix(b"Directive(s):"))[0].split()
    )


class StripCoffExportsTests(unittest.TestCase):
    def test_export_filter_preserves_other_directives(self):
        directives = (
            b' /DEFAULTLIB:libcmt /EXPORT:"?function@@YAHXZ" -export:data,DATA /INCLUDE:keep '
            b'/FAILIFMISMATCH:"RuntimeLibrary=MT_StaticRelease" /ALTERNATENAME:weak=fallback '
        )
        changed, count = EXPORTS.filter_directives(directives)
        self.assertEqual(count, 2)
        self.assertEqual(len(changed), len(directives))
        for directive in [
            b"/DEFAULTLIB:libcmt",
            b"/INCLUDE:keep",
            b'/FAILIFMISMATCH:"RuntimeLibrary=MT_StaticRelease"',
            b"/ALTERNATENAME:weak=fallback",
        ]:
            self.assertIn(directive, changed)
        self.assertNotIn(b"/EXPORT:", changed)
        self.assertNotIn(b"-export:", changed)
        self.assertEqual(EXPORTS.filter_directives(changed), (changed, 0))

    @unittest.skipUnless(shutil.which("llvm-ar"), "Requires llvm-ar")
    def test_invalid_archive_is_not_modified(self):
        original = b"not an archive"
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "invalid.lib"
            path.write_bytes(original)
            with self.assertRaises(subprocess.CalledProcessError):
                EXPORTS.strip_exports(path)
            self.assertEqual(path.read_bytes(), original)

    @unittest.skipUnless(all(shutil.which(tool) for tool in LLVM_TOOLS), "Requires the LLVM Windows tools")
    def test_real_archive_relinks_without_exports_and_preserves_shared_objects(self):
        source = """
#if defined(_M_IX86)
#pragma comment(linker, "/include:_keep")
#pragma comment(linker, "/alternatename:_weak_symbol=_fallback")
#else
#pragma comment(linker, "/include:keep")
#pragma comment(linker, "/alternatename:weak_symbol=fallback")
#endif
#pragma detect_mismatch("fixture", "release")
__declspec(dllexport) volatile unsigned char unused_data[32768] = {1};
__declspec(dllexport) int unused_function(void) { return unused_data[0]; }
int fallback = 42;
extern int weak_symbol;
int keep(void) { return 17; }
__declspec(dllexport) int extension_init(void) { return weak_symbol; }
"""
        for arch, triple in [
            ("x86", "i686-pc-windows-msvc"),
            ("x64", "x86_64-pc-windows-msvc"),
            ("arm64", "aarch64-pc-windows-msvc"),
        ]:
            with self.subTest(arch=arch), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                (root / "fixture.c").write_text(source)

                def run(*command):
                    result = subprocess.run(command, cwd=root, capture_output=True, text=True)
                    self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                    return result.stdout

                run("clang-cl", f"--target={triple}", "/nologo", "/c", "/O2", "/Gy", "/Gw", "/GS-", "fixture.c")
                object_before = (root / "fixture.obj").read_bytes()
                run("llvm-lib", "/out:fixture.lib", "fixture.obj")
                original_directives = non_export_directives(root / "fixture.lib")
                common = [
                    "lld-link",
                    f"/machine:{arch}",
                    "/dll",
                    "/noentry",
                    "/nodefaultlib",
                    "/opt:ref,icf",
                    "/safeseh:no",
                ]
                run(*common, "/out:before.dll", "fixture.lib", "/export:extension_init")
                self.assertEqual(EXPORTS.strip_exports(root / "fixture.lib"), 3)
                self.assertEqual((root / "fixture.obj").read_bytes(), object_before)
                self.assertEqual(non_export_directives(root / "fixture.lib"), original_directives)
                self.assertEqual(EXPORTS.strip_exports(root / "fixture.lib"), 0)
                run(*common, "/out:after.dll", "/map:after.map", "fixture.lib", "/export:extension_init")
                run(*common, "/out:shared.dll", "fixture.obj")
                before = run("llvm-readobj", "--coff-exports", "before.dll")
                after = run("llvm-readobj", "--coff-exports", "after.dll")
                shared = run("llvm-readobj", "--coff-exports", "shared.dll")
                self.assertIn("Name: unused_function", before)
                self.assertIn("Name: unused_data", before)
                self.assertNotIn("Name: unused_function", after)
                self.assertNotIn("Name: unused_data", after)
                self.assertIn("Name: extension_init", after)
                self.assertIn("Name: unused_function", shared)
                self.assertIn("Name: unused_data", shared)
                mapping = (root / "after.map").read_text()
                for symbol in ["keep", "fallback"]:
                    self.assertRegex(mapping, r"\s_?" + symbol + r"\s")
                self.assertNotIn("unused_function", mapping)
                self.assertNotIn("unused_data", mapping)
                self.assertLess((root / "after.dll").stat().st_size, (root / "before.dll").stat().st_size - 30000)

    @unittest.skipUnless(all(shutil.which(tool) for tool in LLVM_TOOLS), "Requires the LLVM Windows tools")
    def test_duplicate_basenames_without_directives_rebuild_and_link(self):
        with tempfile.TemporaryDirectory(prefix="duckdb archive ") as directory:
            root = Path(directory)

            def run(*command, cwd=root):
                result = subprocess.run(command, cwd=cwd, capture_output=True, text=True)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                return result.stdout

            originals = []
            for folder, function, value in [("a", "first", 1), ("b", "second", 2)]:
                (root / folder).mkdir()
                (root / folder / "same.c").write_text(f"int {function}(void) {{ return {value}; }}\n")
                run(
                    "clang-cl",
                    "--target=x86_64-pc-windows-msvc",
                    "/nologo",
                    "/c",
                    "/Zl",
                    "/GS-",
                    "same.c",
                    cwd=root / folder,
                )
                originals.append((root / folder / "same.obj").read_bytes())
            archive = root / "same names.lib"
            run("llvm-lib", f"/out:{archive}", "a/same.obj", "b/same.obj")
            self.assertEqual(non_export_directives(archive), Counter())
            self.assertEqual(EXPORTS.strip_exports(archive), 0)
            symbols = run("llvm-readobj", "--symbols", str(archive))
            self.assertIn("Name: first", symbols)
            self.assertIn("Name: second", symbols)
            self.assertEqual([(root / folder / "same.obj").read_bytes() for folder in ["a", "b"]], originals)
            run(
                "lld-link",
                "/dll",
                "/noentry",
                "/nodefaultlib",
                "/out:check.dll",
                "/implib:check-imports.lib",
                "/export:first",
                "/export:second",
                str(archive),
            )

    @unittest.skipUnless(all(shutil.which(tool) for tool in LLVM_TOOLS), "Requires the LLVM Windows tools")
    def test_failed_librarian_does_not_modify_archive(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "fixture.c").write_text("__declspec(dllexport) int unused(void) { return 42; }\n")
            for command in [
                ["clang-cl", "--target=x86_64-pc-windows-msvc", "/nologo", "/c", "fixture.c"],
                ["llvm-lib", "/out:fixture.lib", "fixture.obj"],
            ]:
                result = subprocess.run(command, cwd=root, capture_output=True, text=True)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            archive = root / "fixture.lib"
            original = archive.read_bytes()
            with self.assertRaises(subprocess.CalledProcessError):
                EXPORTS.strip_exports(archive, librarian=sys.executable)
            self.assertEqual(archive.read_bytes(), original)
            self.assertEqual(list(root.glob("duckdb-exports-*")), [])


@unittest.skipUnless(shutil.which("cmake"), "Requires CMake")
class WindowsStaticExportConfigureTests(unittest.TestCase):
    def test_missing_llvm_does_not_require_python(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "CMakeLists.txt").write_text(
                "cmake_minimum_required(VERSION 3.20)\n"
                "project(fixture NONE)\n"
                "set(WIN32 TRUE)\n"
                "set(MSVC TRUE)\n"
                "set(Python3_FOUND FALSE)\n"
                "option(WINDOWS_STATIC_LIBRARY_EXPORTS \"Keep exports\" OFF)\n"
                + f'include("{ROOT.as_posix()}/extension/extension_linking.cmake")\n'
                + f'duckdb_add_windows_static_export_archive(duckdb_static duckdb_static_no_exports "{SCRIPT.as_posix()}")\n'
                + "\nif(TARGET duckdb_static_no_exports)\n"
                '  message(FATAL_ERROR "Unexpected filtered archive without LLVM")\n'
                "endif()\n"
            )
            result = subprocess.run(
                [
                    "cmake",
                    "-S",
                    str(root),
                    "-B",
                    str(root / "build"),
                    "-DLLVM_AR_EXECUTABLE=OFF",
                    "-DLLVM_OBJCOPY_EXECUTABLE=OFF",
                ],
                capture_output=True,
                text=True,
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertIn("keeping core exports in Windows extensions", result.stdout)


@unittest.skipUnless(
    all(shutil.which(tool) for tool in CMAKE_TOOLS), "Requires CMake, Ninja and the LLVM Windows tools"
)
class WindowsStaticExtensionLinkTests(unittest.TestCase):
    def test_archive_loadables_select_core_after_targets_exist(self):
        for triple, platform in [
            ("x86_64-pc-windows-msvc", "windows_amd64"),
            ("aarch64-pc-windows-msvc", "windows_arm64"),
        ]:
            with self.subTest(platform=platform):
                self.check_archive_loadables(triple, platform)

    def check_archive_loadables(self, triple, platform):
        with tempfile.TemporaryDirectory(prefix="duckdb linking ") as directory:
            root = Path(directory)
            (root / "scripts").mkdir()
            shutil.copyfile(ROOT / "scripts/strip_coff_exports.py", root / "scripts/strip_coff_exports.py")
            (root / "core.c").write_text(
                "__declspec(dllexport) volatile unsigned char unused_data[32768] = {1};\n"
                "__declspec(dllexport) int unused_function(void) { return unused_data[0]; }\n"
                "__declspec(dllexport) int used(void) { return 42; }\n"
                "__declspec(dllexport) volatile int used_data = 7;\n"
                "__declspec(dllexport) int force(void) { return 0; }\n"
            )
            (root / "extension.c").write_text(
                "int used(void);\n__declspec(dllexport) int fixture_duckdb_cpp_init(void) { return used(); }\n"
            )
            (root / "direct.c").write_text(
                '#include "duckdb/common/winapi.hpp"\n'
                "DUCKDB_API int used(void);\n"
                "DUCKDB_API extern volatile int used_data;\n"
                "int force(void);\n"
                "DUCKDB_EXTENSION_API int direct_duckdb_cpp_init(void) { return used() + used_data + force(); }\n"
            )
            (root / "toolchain.cmake").write_text(
                "set(CMAKE_SYSTEM_NAME Windows)\n"
                f'set(CMAKE_C_COMPILER "{Path(shutil.which("clang-cl")).as_posix()}")\n'
                f'set(CMAKE_CXX_COMPILER "{Path(shutil.which("clang-cl")).as_posix()}")\n'
                f"set(CMAKE_C_COMPILER_TARGET {triple})\n"
                f"set(CMAKE_CXX_COMPILER_TARGET {triple})\n"
                f'set(CMAKE_AR "{Path(shutil.which("llvm-lib")).as_posix()}")\n'
                f'set(CMAKE_LINKER "{Path(shutil.which("lld-link")).as_posix()}")\n'
                "set(CMAKE_TRY_COMPILE_TARGET_TYPE STATIC_LIBRARY)\n"
            )
            (root / "CMakeLists.txt").write_text(
                """cmake_minimum_required(VERSION 3.20)
project(fixture C CXX)
find_package(Python3 REQUIRED)
option(WINDOWS_STATIC_LIBRARY_EXPORTS "Keep exports" OFF)
option(EXTENSION_STATIC_BUILD "Embed core" ON)
add_compile_options(/Gy /Gw /GS-)
add_compile_definitions(DUCKDB_BUILD_LIBRARY)
function(link_threads LIBRARY LINKAGE)
endfunction()
function(duckdb_make_native_lto_archive TARGET)
endfunction()
function(duckdb_add_extension_describe NAME KIND)
endfunction()
set(EXTENSION_CONFIG_BUILD OFF)
set(EXTENSION_TESTS_ONLY ON)
set(DUCKDB_NORMALIZED_VERSION fixture)
set(DUCKDB_MODULE_BASE_DIR "@REPO@")
set(DuckDB_BINARY_DIR "${CMAKE_BINARY_DIR}")
file(WRITE "${CMAKE_BINARY_DIR}/duckdb_platform_out" @PLATFORM@)
add_custom_target(duckdb_platform)
include("@REPO@/extension/extension_linking.cmake")
# Match DuckDB's configure order: extensions precede src/CMakeLists.txt.
build_extension_library(fixture extension.c)
build_loadable_extension_directory(direct CPP extension/direct "" "" "" direct.c)
target_include_directories(direct_loadable_extension PRIVATE "@REPO@/src/include")
target_link_options(direct_loadable_extension PRIVATE "/MAP:${CMAKE_BINARY_DIR}/direct.map")
add_library(core_objects OBJECT core.c)
set(ALL_OBJECT_FILES $<TARGET_OBJECTS:core_objects>)
add_library(duckdb_static STATIC ${ALL_OBJECT_FILES})
duckdb_add_windows_static_export_archive(duckdb_static duckdb_static_no_exports "${CMAKE_SOURCE_DIR}/scripts/strip_coff_exports.py")
add_library(host SHARED extension.c)
target_link_libraries(host fixture_extension)
add_library(duckdb SHARED ${ALL_OBJECT_FILES})
foreach(target host fixture_loadable_extension direct_loadable_extension duckdb)
  target_link_options(${target} PRIVATE /noentry /nodefaultlib /safeseh:no /opt:ref,icf)
endforeach()
set(CORE_DEPENDENCY_TARGET fixture_extension)
if(TARGET fixture_prebuilt_extension)
  set(CORE_DEPENDENCY_TARGET fixture_prebuilt_extension)
endif()
file(GENERATE OUTPUT "${CMAKE_BINARY_DIR}/extension-core.txt"
  CONTENT "$<TARGET_GENEX_EVAL:fixture_loadable_extension,$<TARGET_PROPERTY:${CORE_DEPENDENCY_TARGET},INTERFACE_LINK_LIBRARIES>>"
  TARGET fixture_loadable_extension)
file(GENERATE OUTPUT "${CMAKE_BINARY_DIR}/host-core.txt"
  CONTENT "$<TARGET_GENEX_EVAL:host,$<TARGET_PROPERTY:${CORE_DEPENDENCY_TARGET},INTERFACE_LINK_LIBRARIES>>"
  TARGET host)
""".replace(
                    "@REPO@", ROOT.as_posix()
                ).replace(
                    "@PLATFORM@", platform
                )
            )

            def run(*command):
                result = subprocess.run(command, capture_output=True, text=True)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                return result.stdout

            build = root / "build"
            original = None
            for keep, static, prebuilt, ar, objcopy in [
                ("ON", "ON", False, True, True),
                ("OFF", "ON", False, True, True),
                ("ON", "ON", False, True, True),
                ("OFF", "ON", False, True, True),
                ("OFF", "ON", False, False, True),
                ("OFF", "ON", False, True, False),
                ("OFF", "ON", False, False, False),
                ("OFF", "ON", False, True, True),
                ("ON", "ON", True, True, True),
                ("OFF", "ON", True, True, True),
                ("ON", "OFF", False, True, True),
                ("OFF", "OFF", False, True, True),
            ]:
                with self.subTest(exports=keep, static=static, prebuilt=prebuilt, ar=ar, objcopy=objcopy):
                    configured = run(
                        "cmake",
                        "-S",
                        str(root),
                        "-B",
                        str(build),
                        "-G",
                        "Ninja",
                        "-DCMAKE_BUILD_TYPE=Release",
                        "-DCMAKE_C_STANDARD_LIBRARIES=",
                        "-DCMAKE_CXX_STANDARD_LIBRARIES=",
                        f"-DCMAKE_TOOLCHAIN_FILE={root / 'toolchain.cmake'}",
                        f"-DWINDOWS_STATIC_LIBRARY_EXPORTS={keep}",
                        f"-DEXTENSION_STATIC_BUILD={static}",
                        f"-DDUCKDB_EXTENSION_FIXTURE_PREBUILT_PATH={root / 'prebuilt.lib' if prebuilt else ''}",
                        f"-DLLVM_AR_EXECUTABLE={shutil.which('llvm-ar') if ar else 'OFF'}",
                        f"-DLLVM_OBJCOPY_EXECUTABLE={shutil.which('llvm-objcopy') if objcopy else 'OFF'}",
                    )
                    stripped = keep == "OFF" and ar and objcopy
                    if keep == "OFF" and not stripped:
                        self.assertIn("keeping core exports in Windows extensions", configured)
                    self.assertIn(f"WINDOWS_STATIC_LIBRARY_EXPORTS:BOOL={keep}", (build / "CMakeCache.txt").read_text())
                    run("cmake", "--build", str(build))
                    archive = build / "duckdb_static.lib"
                    obj = build / "CMakeFiles/core_objects.dir/core.c.obj"
                    current = (
                        archive.read_bytes(),
                        archive.stat().st_mtime_ns,
                        obj.read_bytes(),
                        obj.stat().st_mtime_ns,
                    )
                    if original is None:
                        original = current
                        shutil.copyfile(build / "fixture_extension.lib", root / "prebuilt.lib")
                    self.assertEqual(current, original)
                    host = run("llvm-readobj", "--coff-exports", str(build / "host.dll"))
                    dll = run("llvm-readobj", "--coff-exports", str(build / "duckdb.dll"))
                    extension_path = build / "extension/fixture/fixture.duckdb_extension"
                    extension = run("llvm-readobj", "--coff-exports", str(extension_path))
                    self.assertIn("Name: unused_function", host)
                    self.assertIn("Name: unused_data", host)
                    self.assertIn("Name: unused_function", dll)
                    self.assertIn("Name: unused_data", dll)
                    self.assertIn("Name: fixture_duckdb_cpp_init", extension)
                    exported = not stripped and static == "ON"
                    self.assertEqual("Name: unused_function" in extension, exported)
                    self.assertEqual("Name: unused_data" in extension, exported)
                    dependency = (build / "extension-core.txt").read_text()
                    if static == "OFF":
                        self.assertEqual(dependency, "")
                    else:
                        self.assertEqual(dependency, "duckdb_static" if exported else "duckdb_static_no_exports")
                    self.assertEqual((build / "host-core.txt").read_text(), "duckdb_static")
                    if stripped and static == "ON":
                        self.assertGreater((build / "host.dll").stat().st_size - extension_path.stat().st_size, 30000)
                    direct_path = build / "extension/direct/direct.duckdb_extension"
                    direct = run("llvm-readobj", "--coff-exports", str(direct_path))
                    self.assertIn("Name: direct_duckdb_cpp_init", direct)
                    self.assertEqual("Name: used\n" in direct, exported)
                    self.assertEqual("Name: used_data\n" in direct, exported)
                    self.assertEqual("Name: unused_function" in direct, exported)
                    self.assertEqual("Name: unused_data" in direct, exported)
                    direct_map = (build / "direct.map").read_text()
                    if static == "ON":
                        # References must retain core code/data even after their exports are removed.
                        self.assertRegex(direct_map, r"\sused\s")
                        self.assertRegex(direct_map, r"\sused_data\s")
                        self.assertNotIn("__imp_used", direct_map)
                    else:
                        imports = run("llvm-readobj", "--coff-imports", str(direct_path))
                        self.assertIn("Name: duckdb.dll", imports)
                        self.assertIn("Symbol: used ", imports)
                        self.assertIn("Symbol: used_data ", imports)

            run(
                "cmake",
                "-S",
                str(root),
                "-B",
                str(build),
                "-DWINDOWS_STATIC_LIBRARY_EXPORTS=OFF",
                "-DEXTENSION_STATIC_BUILD=ON",
            )
            run("cmake", "--build", str(build))
            self.assertIn("no work to do", run("cmake", "--build", str(build)))
            with (root / "core.c").open("a") as file:
                file.write("\n__declspec(dllexport) int newly_unused(void) { return 91; }\n")
            run("cmake", "--build", str(build))
            extension = run("llvm-readobj", "--coff-exports", str(extension_path))
            host = run("llvm-readobj", "--coff-exports", str(build / "host.dll"))
            self.assertIn("Name: newly_unused", host)
            self.assertNotIn("Name: newly_unused", extension)


if __name__ == "__main__":
    unittest.main()

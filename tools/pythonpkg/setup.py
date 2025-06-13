#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import re

########################################################################
# START VERSIONING LOGIC
#
# Keep this at the top of this file, before setuptools_scm is imported.
#
########################################################################

# MAIN_BRANCH_VERSIONING default should be 'True' for main branch and feature branches
# MAIN_BRANCH_VERSIONING default should be 'False' for release branches
# MAIN_BRANCH_VERSIONING default value needs to keep in sync between:
# - CMakeLists.txt
# - scripts/amalgamation.py
# - scripts/package_build.py
# - tools/pythonpkg/setup.py
MAIN_BRANCH_VERSIONING = True
if os.getenv('MAIN_BRANCH_VERSIONING') == "0":
    MAIN_BRANCH_VERSIONING = False
if os.getenv('MAIN_BRANCH_VERSIONING') == "1":
    MAIN_BRANCH_VERSIONING = True

VERSION_RE = re.compile(r"^(?P<major>[0-9]+)\.(?P<minor>[0-9]+)\.(?P<patch>[0-9]+)$")


def bump_version(base_version: str, distance: int, dirty: bool = False):
    """Bump the version if needed."""
    # Validate the base version (this should never include anything else than X.Y.Z)
    base_version_match = VERSION_RE.match(base_version)
    if not base_version_match:
        raise ValueError(f"Incorrect version format: {base_version} (expected X.Y.Z)")

    major, minor, patch = map(int, base_version_match.groups())

    # If we're exactly on a tag (distance = 0, dirty=False)
    if distance == 0 and not dirty:
        return f"{major}.{minor}.{patch}"

    # Otherwise we're at a distance and / or dirty, and need to bump
    if MAIN_BRANCH_VERSIONING:
        return f"{major}.{minor+1}.0.dev{distance}"
    return f"{major}.{minor}.{patch+1}.dev{distance}"


# Here we handle getting versions from env vars. We only support a single way of
# manually overriding the version, which is through OVERRIDE_GIT_DESCRIBE. If
# SETUPTOOLS_SCM_PRETEND_VERSION* is set we unset it.
SCM_PRETEND_ENV_VAR = "SETUPTOOLS_SCM_PRETEND_VERSION_FOR_DUCKDB"
OVERRIDE_GIT_DESCRIBE_ENV_VAR = "OVERRIDE_GIT_DESCRIBE"
OVERRIDE = os.getenv(OVERRIDE_GIT_DESCRIBE_ENV_VAR)

if OVERRIDE:
    # OVERRIDE_GIT_DESCRIBE_ENV_VAR is set, we'll put it in SCM_PRETEND_ENV_VAR_FOR_DUCKDB
    print(f"[setup.py] Found {OVERRIDE_GIT_DESCRIBE_ENV_VAR}={OVERRIDE}")
    DESCRIBE_RE = re.compile(
        r"""
        ^v(?P<tag>\d+\.\d+\.\d+)        # vX.Y.Z
        (?:-(?P<distance>\d+))?         # optional -N
        (?:-g(?P<hash>[0-9a-fA-F]+))?   # optional -g<sha>
        $""",
        re.VERBOSE,
    )
    match = DESCRIBE_RE.match(OVERRIDE)
    if not match:
        raise ValueError(f"Invalid {OVERRIDE_GIT_DESCRIBE_ENV_VAR}: {OVERRIDE}")

    tag = match["tag"]
    distance = match["distance"]
    commit = match["hash"] and match["hash"].lower()

    # If we get an override we do need to bump
    pep440 = bump_version(tag, int(distance or 0))
    if commit:
        pep440 += f"+g{commit}"

    os.environ[SCM_PRETEND_ENV_VAR] = pep440
    print(f"[setup.py] Injected {SCM_PRETEND_ENV_VAR}={pep440}")
elif SCM_PRETEND_ENV_VAR in os.environ:
    # SCM_PRETEND_ENV_VAR is already set, but we don't allow that
    print(f"[setup.py] WARNING: We do not support {SCM_PRETEND_ENV_VAR}! Removing.")
    del os.environ[SCM_PRETEND_ENV_VAR]


if "SETUPTOOLS_SCM_PRETEND_VERSION" in os.environ:
    print(f"[setup.py] WARNING: We do not support SETUPTOOLS_SCM_PRETEND_VERSION! Removing.")
    del os.environ["SETUPTOOLS_SCM_PRETEND_VERSION"]
########################################################################
# END VERSIONING LOGIC
########################################################################

import ctypes
import platform
import sys
import traceback
from functools import lru_cache
from glob import glob
from os.path import exists
from typing import TextIO

from setuptools import setup, Extension
from setuptools.command.build_ext import build_ext as _build_ext


class CompilerLauncherMixin:
    """Add "compiler launchers" to distutils.

    We use this to be able to run the build using "ccache".

    A compiler launcher is a program that is invoked instead of invoking the
    compiler directly. It is passed the full compiler invocation command line.

    A similar feature exists in CMake, see
    https://cmake.org/cmake/help/latest/prop_tgt/LANG_COMPILER_LAUNCHER.html.
    """

    __is_set_up = False

    def build_extensions(self):
        # Integrate into "build_ext"
        self.__setup()
        super().build_extensions()

    def build_libraries(self):
        # Integrate into "build_clib"
        self.__setup()
        super().build_extensions()

    def __setup(self):
        if self.__is_set_up:
            return
        self.__is_set_up = True
        compiler_launcher = os.getenv("DISTUTILS_C_COMPILER_LAUNCHER")
        if compiler_launcher:

            def spawn_with_compiler_launcher(cmd, **kwargs):
                if platform.system() == 'Windows' and len(' '.join(cmd)) > 32766:
                    raise Exception("command too long: " + ' '.join(cmd))

                exclude_programs = ("link.exe",)
                if not cmd[0].endswith(exclude_programs):
                    cmd = [compiler_launcher] + cmd
                try:
                    return original_spawn(cmd, **kwargs)
                except Exception:
                    traceback.print_exc()
                    raise

            original_spawn = self.compiler.spawn
            self.compiler.spawn = spawn_with_compiler_launcher

            # for some reason, the copy of distutils included with setuptools  (instead of the one that was previously
            # part of the stdlib) pushes the link call over the maximum command line length on windows.
            # We can fix this by using special windows short paths, of the form C:\Program ~1\...

            def link_with_short_paths(
                target_desc,
                objects,
                *args,
                **kwargs,
            ):
                return original_link(target_desc, [get_short_path(x) for x in objects], *args, **kwargs)

            original_link = self.compiler.link
            self.compiler.link = link_with_short_paths


def get_short_path(long_name: str) -> str:
    """
    Gets the short path name of a given long path.
    http://stackoverflow.com/a/23598461/200291
    """

    @lru_cache()
    def get_short_path_name_w():
        from ctypes import wintypes

        kernel32 = ctypes.WinDLL('kernel32', use_last_error=True)
        _GetShortPathNameW = kernel32.GetShortPathNameW
        _GetShortPathNameW.argtypes = [wintypes.LPCWSTR, wintypes.LPWSTR, wintypes.DWORD]
        _GetShortPathNameW.restype = wintypes.DWORD
        return _GetShortPathNameW

    if platform.system() != 'Windows':
        return long_name

    assert exists(long_name), long_name
    gspn = get_short_path_name_w()

    output_buf_size = 0
    while True:
        output_buf = ctypes.create_unicode_buffer(output_buf_size)
        needed = gspn(long_name, output_buf, output_buf_size)
        if output_buf_size >= needed:
            return output_buf.value or long_name
        else:
            output_buf_size = needed


class build_ext(CompilerLauncherMixin, _build_ext):
    pass


lib_name = 'duckdb'

extensions = ['core_functions', 'parquet', 'icu', 'json']

is_android = hasattr(sys, 'getandroidapilevel')
is_pyodide = 'PYODIDE' in os.environ
no_source_wheel = is_pyodide
use_jemalloc = (
    not is_android and not is_pyodide and platform.system() == 'Linux' and platform.architecture()[0] == '64bit'
)

if use_jemalloc:
    extensions.append('jemalloc')

unity_build = 0
if 'DUCKDB_BUILD_UNITY' in os.environ:
    unity_build = 16

try:
    import pybind11
except ImportError:
    raise Exception(
        'pybind11 could not be imported. This usually means you\'re calling setup.py directly, or using a version of pip that doesn\'t support PEP517'
    ) from None

# speed up compilation with: -j = cpu_number() on non Windows machines
if os.name != 'nt' and os.environ.get('DUCKDB_DISABLE_PARALLEL_COMPILE', '') != '1':
    from pybind11.setup_helpers import ParallelCompile

    ParallelCompile().install()


def open_utf8(fpath, flags):
    import sys

    if sys.version_info[0] < 3:
        return open(fpath, flags)
    else:
        return open(fpath, flags, encoding="utf8")


# make sure we are in the right directory
os.chdir(os.path.dirname(os.path.realpath(__file__)))

if os.name == 'nt':
    # windows:
    toolchain_args = ['/wd4244', '/wd4267', '/wd4200', '/wd26451', '/wd26495', '/D_CRT_SECURE_NO_WARNINGS', '/utf-8']
else:
    # macos/linux
    toolchain_args = ['-std=c++11', '-g0']
    if 'DUCKDEBUG' in os.environ:
        toolchain_args = ['-std=c++11', '-Wall', '-O0', '-g']
if 'DUCKDB_INSTALL_USER' in os.environ and 'install' in sys.argv:
    sys.argv.append('--user')

existing_duckdb_dir = ''
libraries = []
if 'DUCKDB_BINARY_DIR' in os.environ:
    existing_duckdb_dir = os.environ['DUCKDB_BINARY_DIR']
if 'DUCKDB_COMPILE_FLAGS' in os.environ:
    toolchain_args = os.environ['DUCKDB_COMPILE_FLAGS'].split()
if 'DUCKDB_LIBS' in os.environ:
    libraries = os.environ['DUCKDB_LIBS'].split(' ')

define_macros = [('DUCKDB_PYTHON_LIB_NAME', lib_name)]

custom_platform = os.environ.get('DUCKDB_CUSTOM_PLATFORM')
if custom_platform is not None:
    define_macros.append(('DUCKDB_CUSTOM_PLATFORM', custom_platform))

if platform.system() == 'Darwin':
    toolchain_args.extend(['-stdlib=libc++', '-mmacosx-version-min=10.7'])

if platform.system() == 'Windows':
    define_macros.extend([('DUCKDB_BUILD_LIBRARY', None), ('WIN32', None)])

if is_pyodide:
    # show more useful error messages in the browser
    define_macros.append(('PYBIND11_DETAILED_ERROR_MESSAGES', None))

if 'BUILD_HTTPFS' in os.environ:
    libraries += ['crypto', 'ssl']
    extensions += ['httpfs']

for ext in extensions:
    define_macros.append(('DUCKDB_EXTENSION_{}_LINKED'.format(ext.upper()), None))

if not is_pyodide:
    # currently pyodide environment is not compatible with dynamic extension loading
    define_macros.extend([('DUCKDB_EXTENSION_AUTOLOAD_DEFAULT', '1'), ('DUCKDB_EXTENSION_AUTOINSTALL_DEFAULT', '1')])

linker_args = toolchain_args[:]
if platform.system() == 'Windows':
    linker_args.extend(['rstrtmgr.lib', 'bcrypt.lib'])

short_paths = False
if platform.system() == 'Windows':
    short_paths = True

extra_files = []
header_files = []


def list_source_files(directory):
    sources = glob('src/**/*.cpp', recursive=True)
    return sources


script_path = os.path.dirname(os.path.abspath(__file__))
main_include_path = os.path.join(script_path, 'src', 'include')
main_source_path = os.path.join(script_path, 'src')
main_source_files = ['duckdb_python.cpp'] + list_source_files(main_source_path)

include_directories = [main_include_path, pybind11.get_include(False), pybind11.get_include(True)]
if 'BUILD_HTTPFS' in os.environ and 'OPENSSL_ROOT_DIR' in os.environ:
    include_directories += [os.path.join(os.environ['OPENSSL_ROOT_DIR'], 'include')]


def exclude_extensions(f: TextIO):
    files = [x for x in f.read().split('\n') if len(x) > 0]
    if use_jemalloc:
        return files
    else:
        return [x for x in files if 'jemalloc' not in x]


if len(existing_duckdb_dir) == 0:
    # no existing library supplied: compile everything from source
    source_files = main_source_files

    # check if amalgamation exists
    if os.path.isfile(os.path.join(script_path, '..', '..', 'scripts', 'amalgamation.py')):
        # amalgamation exists: compiling from source directory
        # copy all source files to the current directory
        sys.path.append(os.path.join(script_path, '..', '..', 'scripts'))
        import package_build

        (source_list, include_list, original_sources) = package_build.build_package(
            os.path.join(script_path, "duckdb_build"), extensions, False, unity_build, "duckdb_build", short_paths
        )

        duckdb_sources = [
            os.path.sep.join(package_build.get_relative_path(script_path, x).split('/')) for x in source_list
        ]
        duckdb_sources.sort()

        original_sources = [os.path.join("duckdb_build", x) for x in original_sources]

        duckdb_includes = [os.path.join("duckdb_build", x) for x in include_list]
        duckdb_includes += ["duckdb_build"]

        # gather the include files
        import amalgamation

        header_files = amalgamation.list_includes_files(duckdb_includes)

        # write the source list, include list and git hash to separate files
        with open_utf8('sources.list', 'w+') as f:
            for source_file in duckdb_sources:
                f.write(source_file + "\n")

        with open_utf8('includes.list', 'w+') as f:
            for include_file in duckdb_includes:
                f.write(include_file + '\n')

        extra_files = ['sources.list', 'includes.list'] + original_sources
    else:
        # if amalgamation does not exist, we are in a package distribution
        # read the include files, source list and include files from the supplied lists
        with open_utf8('sources.list', 'r') as f:
            duckdb_sources = exclude_extensions(f)

        with open_utf8('includes.list', 'r') as f:
            duckdb_includes = exclude_extensions(f)

    source_files += duckdb_sources
    include_directories = duckdb_includes + include_directories

    libduckdb = Extension(
        lib_name + '.duckdb',
        include_dirs=include_directories,
        sources=source_files,
        extra_compile_args=toolchain_args,
        extra_link_args=linker_args,
        libraries=libraries,
        language='c++',
        define_macros=define_macros,
    )
else:
    sys.path.append(os.path.join(script_path, '..', '..', 'scripts'))
    import package_build

    include_directories += [os.path.join('..', '..', include) for include in package_build.third_party_includes()]
    toolchain_args += ['-I' + x for x in package_build.includes(extensions)]

    result_libraries = package_build.get_libraries(existing_duckdb_dir, libraries, extensions)
    library_dirs = [x[0] for x in result_libraries if x[0] is not None]
    libnames = [x[1] for x in result_libraries if x[1] is not None]

    libduckdb = Extension(
        lib_name + '.duckdb',
        include_dirs=include_directories,
        sources=main_source_files,
        extra_compile_args=toolchain_args,
        extra_link_args=linker_args,
        libraries=libnames,
        library_dirs=library_dirs,
        language='c++',
        define_macros=define_macros,
    )


# data files need to be formatted as [(directory, [files...]), (directory2, [files...])]
# no clue why the setup script can't do this automatically, but hey
def setup_data_files(data_files):
    directory_map = {}
    for data_file in data_files:
        normalized_fpath = os.path.sep.join(data_file.split('/'))
        splits = normalized_fpath.rsplit(os.path.sep, 1)
        if len(splits) == 1:
            # no directory specified
            directory = ""
            fname = normalized_fpath
        else:
            directory = splits[0]
            fname = splits[1]
        if directory not in directory_map:
            directory_map[directory] = []
        directory_map[directory].append(normalized_fpath)
    new_data_files = []
    for kv in directory_map.keys():
        new_data_files.append((kv, directory_map[kv]))
    return new_data_files


data_files = setup_data_files(extra_files + header_files)
if no_source_wheel:
    data_files = []

packages = [
    lib_name,
    'duckdb.typing',
    'duckdb.query_graph',
    'duckdb.functional',
    'duckdb.value',
    'duckdb-stubs',
    'duckdb-stubs.functional',
    'duckdb-stubs.typing',
    'duckdb-stubs.value',
    'duckdb-stubs.value.constant',
    'adbc_driver_duckdb',
]

spark_packages = [
    'duckdb.experimental',
    'duckdb.experimental.spark',
    'duckdb.experimental.spark.sql',
    'duckdb.experimental.spark.errors',
    'duckdb.experimental.spark.errors.exceptions',
]

packages.extend(spark_packages)

setup(
    name="duckdb",  # Needed to make SETUPTOOLS_SCM_PRETEND_VERSION_FOR_DUCKDB work
    data_files=data_files,
    # NOTE: might need to be find_packages() ?
    packages=packages,
    include_package_data=True,
    long_description="See here for an introduction: https://duckdb.org/docs/stable/clients/python/overview",
    ext_modules=[libduckdb],
    use_scm_version={
        "version_scheme": lambda v: bump_version(str(v.tag), v.distance, v.dirty),
        "root": "../..",
        "fallback_version": "0.0.0",
        "local_scheme": "no-local-version",
    },
    cmdclass={"build_ext": build_ext},
)

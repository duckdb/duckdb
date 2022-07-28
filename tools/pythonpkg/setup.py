#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import platform
import multiprocessing.pool

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

            def spawn_with_compiler_launcher(cmd):
                exclude_programs = ("link.exe",)
                if not cmd[0].endswith(exclude_programs):
                    cmd = [compiler_launcher] + cmd
                return original_spawn(cmd)

            original_spawn = self.compiler.spawn
            self.compiler.spawn = spawn_with_compiler_launcher


class build_ext(CompilerLauncherMixin, _build_ext):
    pass


lib_name = 'duckdb'

extensions = ['parquet', 'icu', 'fts', 'tpch', 'tpcds', 'visualizer', 'json', 'excel']

if platform.system() == 'Windows':
    extensions = ['parquet', 'icu', 'fts', 'tpch', 'json', 'excel']

unity_build = 0
if 'DUCKDB_BUILD_UNITY' in os.environ:
    unity_build = 16

def parallel_cpp_compile(self, sources, output_dir=None, macros=None, include_dirs=None, debug=0,
                         extra_preargs=None, extra_postargs=None, depends=None):
    # Copied from distutils.ccompiler.CCompiler
    macros, objects, extra_postargs, pp_opts, build = self._setup_compile(
        output_dir, macros, include_dirs, sources, depends, extra_postargs)

    cc_args = self._get_cc_args(pp_opts, debug, extra_preargs)

    def _single_compile(obj):
        try:
            src, ext = build[obj]
        except KeyError:
            return
        self._compile(obj, src, ext, cc_args, extra_postargs, pp_opts)

    list(multiprocessing.pool.ThreadPool(multiprocessing.cpu_count()).imap(_single_compile, objects))
    return objects


# speed up compilation with: -j = cpu_number() on non Windows machines
if os.name != 'nt':
    import distutils.ccompiler
    distutils.ccompiler.CCompiler.compile = parallel_cpp_compile

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
    toolchain_args = ['/wd4244', '/wd4267', '/wd4200', '/wd26451', '/wd26495', '/D_CRT_SECURE_NO_WARNINGS']
else:
    # macos/linux
    toolchain_args = ['-std=c++11', '-g0']
    if 'DUCKDEBUG' in os.environ:
        toolchain_args = ['-std=c++11', '-Wall', '-O0', '-g']
if 'DUCKDB_INSTALL_USER' in os.environ and 'install' in sys.argv:
    sys.argv.append('--user')

existing_duckdb_dir = ''
new_sys_args = []
libraries = []
for i in range(len(sys.argv)):
    if sys.argv[i].startswith("--binary-dir="):
        existing_duckdb_dir = sys.argv[i].split('=', 1)[1]
    elif sys.argv[i].startswith('--package_name=') :
        lib_name = sys.argv[i].split('=', 1)[1]
    elif sys.argv[i].startswith("--compile-flags="):
        toolchain_args = ['-std=c++11'] + [x.strip() for x in sys.argv[i].split('=', 1)[1].split(' ') if len(x.strip()) > 0]
    elif sys.argv[i].startswith("--libs="):
        libraries = [x.strip() for x in sys.argv[i].split('=', 1)[1].split(' ') if len(x.strip()) > 0]
    else:
        new_sys_args.append(sys.argv[i])
sys.argv = new_sys_args
toolchain_args.append('-DDUCKDB_PYTHON_LIB_NAME='+lib_name)

if platform.system() == 'Darwin':
    toolchain_args.extend(['-stdlib=libc++', '-mmacosx-version-min=10.7'])

if platform.system() == 'Windows':
    toolchain_args.extend(['-DDUCKDB_BUILD_LIBRARY','-DWIN32'])

if 'BUILD_HTTPFS' in os.environ:
    libraries += ['crypto', 'ssl']
    extensions += ['httpfs']

for ext in extensions:
    toolchain_args.extend(['-DBUILD_{}_EXTENSION'.format(ext.upper())])

class get_pybind_include(object):
    def __init__(self, user=False):
        self.user = user

    def __str__(self):
        import pybind11
        return pybind11.get_include(self.user)

class get_numpy_include(object):
    def __str__(self):
        import numpy
        return numpy.get_include()

extra_files = []
header_files = []

script_path = os.path.dirname(os.path.abspath(__file__))
main_include_path = os.path.join(script_path, 'src', 'include')
main_source_path = os.path.join(script_path, 'src')
main_source_files = ['duckdb_python.cpp'] + [os.path.join('src', x) for x in os.listdir(main_source_path) if '.cpp' in x]
include_directories = [main_include_path, get_numpy_include(), get_pybind_include(), get_pybind_include(user=True)]

if len(existing_duckdb_dir) == 0:
    # no existing library supplied: compile everything from source
    source_files = main_source_files

    # check if amalgamation exists
    if os.path.isfile(os.path.join(script_path, '..', '..', 'scripts', 'amalgamation.py')):
        # amalgamation exists: compiling from source directory
        # copy all source files to the current directory
        sys.path.append(os.path.join(script_path, '..', '..', 'scripts'))
        import package_build
        (source_list, include_list, original_sources) = package_build.build_package(os.path.join(script_path, lib_name), extensions, False, unity_build)

        duckdb_sources = [os.path.sep.join(package_build.get_relative_path(script_path, x).split('/')) for x in source_list]
        duckdb_sources.sort()

        original_sources = [os.path.join(lib_name, x) for x in original_sources]

        duckdb_includes = [os.path.join(lib_name, x) for x in include_list]
        duckdb_includes += [lib_name]

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
            duckdb_sources = [x for x in f.read().split('\n') if len(x) > 0]

        with open_utf8('includes.list', 'r') as f:
            duckdb_includes = [x for x in f.read().split('\n') if len(x) > 0]

    source_files += duckdb_sources
    include_directories = duckdb_includes + include_directories

    libduckdb = Extension(lib_name,
        include_dirs=include_directories,
        sources=source_files,
        extra_compile_args=toolchain_args,
        extra_link_args=toolchain_args,
        libraries=libraries,
        language='c++')
else:
    sys.path.append(os.path.join(script_path, '..', '..', 'scripts'))
    import package_build

    include_directories += [os.path.join('..', '..', include) for include in package_build.third_party_includes()]
    toolchain_args += ['-I' + x for x in package_build.includes(extensions)]

    result_libraries = package_build.get_libraries(existing_duckdb_dir, libraries, extensions)
    library_dirs = [x[0] for x in result_libraries if x[0] is not None]
    libnames = [x[1] for x in result_libraries if x[1] is not None]

    libduckdb = Extension(lib_name,
        include_dirs=include_directories,
        sources=main_source_files,
        extra_compile_args=toolchain_args,
        extra_link_args=toolchain_args,
        libraries=libnames,
        library_dirs=library_dirs,
        language='c++')

# Only include pytest-runner in setup_requires if we're invoking tests
if {'pytest', 'test', 'ptr'}.intersection(sys.argv):
    setup_requires = ['pytest-runner']
else:
    setup_requires = []

setuptools_scm_conf = {"root": "../..", "relative_to": __file__}
if os.getenv('SETUPTOOLS_SCM_NO_LOCAL', 'no') != 'no':
    setuptools_scm_conf['local_scheme'] = 'no-local-version'

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

setup(
    name = lib_name,
    description = 'DuckDB embedded database',
    keywords = 'DuckDB Database SQL OLAP',
    url="https://www.duckdb.org",
    long_description = 'See here for an introduction: https://duckdb.org/docs/api/python',
    license='MIT',
    install_requires=[ # these version is still available for Python 2, newer ones aren't
         'numpy>=1.14'
    ],
    data_files = data_files,
    packages=[
        'duckdb_query_graph',
        'duckdb-stubs'
    ],
    include_package_data=True,
    setup_requires=setup_requires + ["setuptools_scm"] + ['pybind11>=2.6.0'],
    use_scm_version = setuptools_scm_conf,
    tests_require=['google-cloud-storage', 'mypy', 'pytest'],
    classifiers = [
        'Topic :: Database :: Database Engines/Servers',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
    ],
    ext_modules = [libduckdb],
    maintainer = "Hannes Muehleisen",
    maintainer_email = "hannes@cwi.nl",
    cmdclass={"build_ext": build_ext},
)

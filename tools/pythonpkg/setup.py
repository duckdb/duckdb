#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import subprocess
import shutil
import platform


from setuptools import setup, Extension
from setuptools.command.sdist import sdist
import distutils.spawn

# make sure we are in the right directory
os.chdir(os.path.dirname(os.path.realpath(__file__)))

toolchain_args = ['-std=c++11', '-g0']
if 'DUCKDEBUG' in os.environ:
    toolchain_args = ['-std=c++11', '-Wall', '-O0', '-g']
if 'DUCKDB_INSTALL_USER' in os.environ:
    sys.argv.append('--user')

existing_duckdb_dir = ''
new_sys_args = []
libraries = []
for i in range(len(sys.argv)):
    recognized = False
    if sys.argv[i].startswith("--binary-dir="):
        existing_duckdb_dir = sys.argv[i].split('=', 1)[1]
        recognized = True
    elif sys.argv[i].startswith("--compile-flags="):
        toolchain_args = ['-std=c++11'] + [x.strip() for x in sys.argv[i].split('=', 1)[1].split(' ') if len(x.strip()) > 0]
        recognized = True
    elif sys.argv[i].startswith("--libs="):
        libraries = [x.strip() for x in sys.argv[i].split('=', 1)[1].split(' ') if len(x.strip()) > 0]
        recognized = True
    if not recognized:
        new_sys_args.append(sys.argv[i])
sys.argv = new_sys_args

if platform.system() == 'Darwin':
    toolchain_args.extend(['-stdlib=libc++', '-mmacosx-version-min=10.7'])

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
include_directories = ['.', get_numpy_include(), get_pybind_include(), get_pybind_include(user=True)]
if len(existing_duckdb_dir) == 0:
    # no existing library supplied: compile everything from source
    source_files = ['duckdb_python.cpp']

    # check if amalgamation exists
    if os.path.isfile(os.path.join(script_path, '..', '..', 'scripts', 'amalgamation.py')):
        # amalgamation exists: compiling from source directory
        # copy all source files to the current directory
        sys.path.append(os.path.join(script_path, '..', '..', 'scripts'))
        import package_build

        (source_list, include_list, githash) = package_build.build_package(os.path.join(script_path, 'duckdb'))

        duckdb_sources = [x.replace(script_path + os.path.sep, '') for x in source_list]
        duckdb_sources.sort()

        duckdb_includes = [os.path.join('duckdb', x) for x in include_list]
        duckdb_includes += ['duckdb']

        # gather the include files
        import amalgamation
        header_files = amalgamation.list_includes_files(duckdb_includes)

        # write the source list, include list and git hash to separate files
        with open('sources.list', 'w+') as f:
            for source_file in duckdb_sources:
                f.write(source_file + "\n")

        with open('includes.list', 'w+') as f:
            for include_file in duckdb_includes:
                f.write(include_file + '\n')

        with open('githash.list', 'w+') as f:
            f.write(githash + '\n')

        extra_files = ['sources.list', 'includes.list', 'githash.list']
    else:
        # if amalgamation does not exist, we are in a package distribution
        # read the include files, source list and include files from the supplied lists
        with open('sources.list', 'r') as f:
            duckdb_sources = [x for x in f.read().split('\n') if len(x) > 0]

        with open('includes.list', 'r') as f:
            duckdb_includes = [x for x in f.read().split('\n') if len(x) > 0]

        with open('githash.list', 'r') as f:
            githash = f.read().strip()

    source_files += duckdb_sources
    include_directories = duckdb_includes + include_directories
    toolchain_args += ['-DDUCKDB_SOURCE_ID="{}"'.format(githash)]

    libduckdb = Extension('duckdb',
        include_dirs=include_directories,
        sources=source_files,
        extra_compile_args=toolchain_args,
        extra_link_args=toolchain_args,
        language='c++')
else:
    sys.path.append(os.path.join(script_path, '..', '..', 'scripts'))
    import package_build

    toolchain_args += ['-I' + x for x in package_build.includes()]

    result_libraries = package_build.get_libraries(existing_duckdb_dir, libraries)
    library_dirs = [x[0] for x in result_libraries if x[0] is not None]
    libnames = [x[1] for x in result_libraries if x[1] is not None]

    libduckdb = Extension('duckdb',
        include_dirs=include_directories,
        sources=['duckdb_python.cpp'],
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

setup(
    name = "duckdb",
    description = 'DuckDB embedded database',
    keywords = 'DuckDB Database SQL OLAP',
    url="https://www.duckdb.org",
    long_description = 'See here for an introduction: https://duckdb.org/docs/api/python',
    install_requires=[ # these version is still available for Python 2, newer ones aren't
         'numpy>=1.14'
    ],
    data_files = extra_files + header_files,
    packages=['duckdb_query_graph'],
    include_package_data=True,
    setup_requires=setup_requires + ["setuptools_scm"] + ['pybind11>=2.4'],
    use_scm_version = setuptools_scm_conf,
    tests_require=['pytest'],
    classifiers = [
        'Topic :: Database :: Database Engines/Servers',
        'Intended Audience :: Developers'
    ],
    ext_modules = [libduckdb],
    maintainer = "Hannes Muehleisen",
    maintainer_email = "hannes@cwi.nl"
)

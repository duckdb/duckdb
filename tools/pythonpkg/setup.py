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

existing_duckdb_dir = ''
if 'EXISTING_DUCKDB' in os.environ:
    existing_duckdb_dir = os.environ['EXISTING_DUCKDB']
    print("Linking to existing DuckDB library: " + existing_duckdb_dir)

# check if amalgamation exists
if os.path.isfile(os.path.join('..', '..', 'scripts', 'amalgamation.py')):
    prev_wd = os.getcwd()
    target_header = os.path.join(prev_wd, 'duckdb.hpp')
    target_source = os.path.join(prev_wd, 'duckdb.cpp')
    os.chdir(os.path.join('..', '..'))
    sys.path.append('scripts')
    import amalgamation
    amalgamation.generate_amalgamation(target_source, target_header)

    sys.path.append('extension/parquet')
    import parquet_amalgamation
    ext_target_header = os.path.join(prev_wd, 'parquet-extension.hpp')
    ext_target_source = os.path.join(prev_wd, 'parquet-extension.cpp')
    parquet_amalgamation.generate_amalgamation(ext_target_source, ext_target_header)

    os.chdir(prev_wd)


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


class get_pyarrow_include(object):
    def __str__(self):
        import pyarrow
        return pyarrow.get_include()


if len(existing_duckdb_dir) == 0:
    # no existing library supplied: compile everything from source
    libduckdb = Extension('duckdb',
        include_dirs=['.', get_numpy_include(), get_pybind_include(), get_pybind_include(user=True), get_pyarrow_include()],
        sources=['duckdb_python.cpp', 'duckdb.cpp', 'parquet-extension.cpp'],
        extra_compile_args=toolchain_args,
        extra_link_args=toolchain_args,
        language='c++')
else:
    # existing lib provided: link to it
    # 'duckdb.cpp', 'parquet-extension.cpp'
    libs = ['-L' + os.path.join(existing_duckdb_dir, 'src'), '-lduckdb_static']
    libs += ['-L' + os.path.join(existing_duckdb_dir, 'extension', 'parquet'), '-lparquet_extension']
    libs += ['-L' + os.path.join(existing_duckdb_dir, 'extension', 'icu'), '-licu_extension']

    libduckdb = Extension('duckdb',
        include_dirs=['.', get_numpy_include(), get_pybind_include(), get_pybind_include(user=True), get_pyarrow_include()],
        sources=['duckdb_python.cpp'],
        extra_compile_args=toolchain_args,
        extra_link_args=toolchain_args + libs,
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

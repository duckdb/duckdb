#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import numpy
import sys
import subprocess
import platform
import shutil

import distutils.spawn
from setuptools import setup, Extension
from setuptools.command.sdist import sdist
from distutils.command.build_ext import build_ext


# make sure we are in the right directory
os.chdir(os.path.dirname(os.path.realpath(__file__)))

ARCHIVE_EXT = 'a'
LIB_PREFIX = 'lib'
if os.name == 'nt':
    ARCHIVE_EXT = 'lib'
    LIB_PREFIX = 'Release/'

DIR_PREFIX = 'src/duckdb'
if not os.path.exists(DIR_PREFIX):
     # this is a build from within the tools/pythonpkg directory
    DIR_PREFIX = '../../'

def get_library_name(lib):
    return LIB_PREFIX + lib + '.' + ARCHIVE_EXT

DEFAULT_BUILD_DIR = os.path.join(DIR_PREFIX, 'build', 'release_notest')

BUILD_DIR = DEFAULT_BUILD_DIR
if 'DUCKDB_PYTHON_TARGET' in os.environ:
    BUILD_DIR = os.environ['DUCKDB_PYTHON_TARGET']


INCLUDE_DIR = os.path.join(DIR_PREFIX,  'src', 'include')

DUCKDB_LIB = os.path.join(BUILD_DIR, 'src', get_library_name('duckdb_static'))
PG_LIB = os.path.join(BUILD_DIR, 'third_party', 'libpg_query', get_library_name('pg_query'))
RE2_LIB = os.path.join(BUILD_DIR, 'third_party', 're2', get_library_name('re2'))
MINIZ_LIB = os.path.join(BUILD_DIR, 'third_party', 'miniz', get_library_name('miniz'))


# wrapper that builds the main DuckDB library first
class CustomBuiltExtCommand(build_ext):
    def build_duckdb(self):
        cmake_bin = distutils.spawn.find_executable('cmake')
        if (cmake_bin is None):
            raise Exception('DuckDB needs cmake to build from source')

        wd = os.getcwd()
        os.chdir(DIR_PREFIX)
        if not os.path.exists('build/release_notest'):
            os.makedirs('build/release_notest')
        os.chdir('build/release_notest')

        configcmd = 'cmake -DCMAKE_BUILD_TYPE=Release -DLEAN=1 ../..'
        buildcmd = 'cmake --build . --target duckdb_static'

        if os.name == 'nt':
            if platform.architecture()[0] == '64bit':
                configcmd += ' -DCMAKE_GENERATOR_PLATFORM=x64'
            buildcmd += ' --config Release'

        subprocess.Popen(configcmd.split(' ')).wait()
        subprocess.Popen(buildcmd.split(' ')).wait()

        os.chdir(wd)

    def run(self):
        if BUILD_DIR == DEFAULT_BUILD_DIR:
            self.build_duckdb()

        for library in [DUCKDB_LIB, PG_LIB, RE2_LIB, MINIZ_LIB]:
            if not os.path.isfile(library):
                raise Exception('Build failed: could not find required library file "%s"' % library)
        print(INCLUDE_DIR)
        build_ext.run(self)

# create a distributable directory structure
class CustomSdistCommand(sdist):
    def run(self):
        if os.path.exists('src/duckdb'):
            shutil.rmtree('src/duckdb')
        if not os.path.exists('src/duckdb/third_party'):
            os.makedirs('src/duckdb/third_party')
        shutil.copyfile('../../CMakeLists.txt', 'src/duckdb/CMakeLists.txt')
        shutil.copyfile('../../third_party/CMakeLists.txt', 'src/duckdb/third_party/CMakeLists.txt')
        shutil.copytree('../../src', 'src/duckdb/src')
        shutil.copytree('../../third_party/libpg_query', 'src/duckdb/third_party/libpg_query')
        shutil.copytree('../../third_party/hyperloglog', 'src/duckdb/third_party/hyperloglog')
        shutil.copytree('../../third_party/re2', 'src/duckdb/third_party/re2')
        shutil.copytree('../../third_party/miniz', 'src/duckdb/third_party/miniz')
        sdist.run(self)

includes = [numpy.get_include(), INCLUDE_DIR, '.']
sources = ['connection.cpp', 'cursor.cpp', 'module.cpp']

toolchain_args = ['-std=c++11', '-Wall']
if platform.system() == 'Darwin':
    toolchain_args.extend(['-stdlib=libc++', '-mmacosx-version-min=10.7'])

libduckdb = Extension('duckdb',
    include_dirs=includes,
    sources=sources,
    extra_compile_args=toolchain_args,
    extra_link_args=toolchain_args,
    language='c++',
    extra_objects=[DUCKDB_LIB, PG_LIB, RE2_LIB, MINIZ_LIB])

# Only include pytest-runner in setup_requires if we're invoking tests
if {'pytest', 'test', 'ptr'}.intersection(sys.argv):
    setup_requires = ['pytest-runner']
else:
    setup_requires = []

setup(
    name = "duckdb",
    description = 'DuckDB embedded database',
    keywords = 'DuckDB Database SQL OLAP',
    url="https://github.com/cwida/duckdb",
    long_description = '',
    install_requires=[ # these versions are still available for Python 2, newer ones aren't
         'numpy>=1.14', 
         'pandas>=0.23'
    ],
    packages=['duckdb_query_graph'],
    include_package_data=True,
    setup_requires=setup_requires + ["setuptools_scm"],
    use_scm_version = {"root": "../..", "relative_to": __file__},
    tests_require=['pytest'],
    classifiers = [
        'Topic :: Database :: Database Engines/Servers',
        'Intended Audience :: Developers',
        'Development Status :: 3 - Alpha'
    ],
    cmdclass={
       'build_ext': CustomBuiltExtCommand,
       'sdist': CustomSdistCommand
    },
    ext_modules = [libduckdb],
    maintainer = "Hannes Muehleisen",
    maintainer_email = "hannes@cwi.nl"
)

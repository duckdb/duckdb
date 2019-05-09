#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import numpy
import sys
from setuptools import setup, Extension
from setuptools.command.install import install
import subprocess
import platform

basedir = os.path.dirname(os.path.realpath(__file__))

if platform.architecture()[0] != '64bit':
    raise Exception('DuckDB only supports 64 bit at this point')

if sys.version_info < (3, 6):
    raise Exception('DuckDB requires at least Python 3.6')

# make sure we are in the right directory
os.chdir(os.path.dirname(os.path.realpath(__file__)))

archive_ext = 'a'
lib_prefix = 'lib'
if os.name == 'nt':
    archive_ext = 'lib'
    lib_prefix = 'RelWithDebInfo/'

# wrapper that builds the main DuckDB library first
class CustomInstallCommand(install):
    def run(self):
        wd = os.getcwd()
        os.chdir('../../')
        os.makedirs('build/release_notest', exist_ok=True)
        os.chdir('build/release_notest')

        configcmd = 'cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo -DLEAN=1 ../..'
        buildcmd = 'cmake --build . --target duckdb_static'

        if os.name == 'nt':
            configcmd += ' -DCMAKE_GENERATOR_PLATFORM=x64'
            buildcmd += ' --config RelWithDebInfo'

        subprocess.Popen(configcmd.split(' ')).wait()
        subprocess.Popen(buildcmd.split(' ')).wait()

        os.chdir(wd)
        if not os.path.isfile('../../build/release_notest/src/%sduckdb_static.%s' % (lib_prefix, archive_ext)):
            raise Exception('Library build failed :/') 
        install.run(self)

includes = [numpy.get_include(), '../../src/include', '.']
sources = ['connection.cpp', 'cursor.cpp', 'module.cpp']

libduckdb = Extension('duckdb',
    include_dirs=includes,
    sources=sources,
    extra_compile_args=['-std=c++11', '-Wall'],
    language='c++', # for linking c++ stdlib
    extra_objects=['../../build/release_notest/src/%sduckdb_static.%s' % (lib_prefix, archive_ext), '../../build/release_notest/third_party/libpg_query/%spg_query.%s' % (lib_prefix, archive_ext), '../../build/release_notest/third_party/re2/%sre2.%s' % (lib_prefix, archive_ext), '../../build/release_notest/third_party/miniz/%sminiz.%s' % (lib_prefix, archive_ext)])

setup(
    name = "duckdb",
    version = '0.0.2',
    description = 'DuckDB embedded database',
    keywords = 'DuckDB Database SQL OLAP',
    url="https://github.com/cwida/duckdb",
    long_description = '',
    install_requires=[
         'numpy>=1.16',
         'pandas>=0.24'
    ],
    setup_requires=['pytest-runner'],
    tests_require=['pytest'],
    classifiers = [
        'Programming Language :: Python :: 3.7',
        'Topic :: Database :: Database Engines/Servers',
        'Intended Audience :: Developers',
        'Development Status :: 3 - Alpha'
    ],
    cmdclass={
       'install': CustomInstallCommand,
    },
    ext_modules = [libduckdb]
)

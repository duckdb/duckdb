#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import numpy
import sys
from setuptools import setup, Extension
from setuptools.command.install import install
import subprocess

basedir = os.path.dirname(os.path.realpath(__file__))

# sort of hacky wrapper that builds the main duckdb library first
class CustomInstallCommand(install):
    def run(self):
        wd = os.getcwd()
        os.chdir("../../")
        process = subprocess.Popen(['make', 'opt'])
        process.wait()
        os.chdir(wd)
        if process.returncode != 0 or not os.path.isfile('../../build/release/src/libduckdb_static.a'):
            raise Exception('Library build failed. :/') 
        install.run(self)

includes = [numpy.get_include(), '../../src/include', '.']
sources = ['connection.c', 'cursor.c', 'module.c', 'pandas.c']

libduckdb = Extension('duckdb', define_macros=[('MODULE_NAME',  '"duckdb"')],
    include_dirs=includes,
    sources=sources,
    extra_compile_args=['-std=c99', '-Wall', '-O0'],
    language='c',
    extra_objects=['../../build/release/src/libduckdb_static.a', '../../build/release/third_party/libpg_query/libpg_query.a'])

setup(
    name = "duckdb",
    version = '0.0.1',
    description = 'DuckDB embedded database',
    author = 'Hannes Mühleisen',
    author_email = 'hannes@cwi.nl',
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
    # ...
        'Development Status :: 3 - Alpha'
    ],
    cmdclass={
       # 'install': CustomInstallCommand,
    },
    ext_modules = [libduckdb]
)

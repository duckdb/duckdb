#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import numpy
import sys
from setuptools import setup, Extension

basedir = os.path.dirname(os.path.realpath(__file__))


# sources = []
# includes = [numpy.get_include()]
sources = ['connection.c', 'cursor.c', 'module.c']
includes = ['../../src/include', '.']
excludes = []

libduckdb = Extension('duckdb', define_macros=[('MODULE_NAME',  '"duckdb"')],
    include_dirs=includes,
    sources=sources,
    extra_compile_args=['-std=c99', '-Wall'],
    language='c',
    extra_objects=['../../build/release/src/libduckdb_static.a', '../../build/release/third_party/libpg_query/libpg_query.a'])

setup(
    name = "duckdb",
    version = '0.0.1',
    description = 'DuckDB embedded database',
    # author = 'Hannes Mühleisen',
    # author_email = 'hannes@cwi.nl',
    keywords = 'DuckDB Database SQL OLAP',
#    packages = ['duckdb'],
    # package_dir = {'': 'lib'},
    url="https://github.com/cwida/duckdb",
    long_description = '',
    # install_requires=[
    #     'numpy>=1.7',
    #     'pandas>=0.20'
    # ],
    setup_requires=['pytest-runner'],
    tests_require=['pytest'],
    # zip_safe = False,
    classifiers = [
    # ...
        'Development Status :: 3 - Alpha'
    ],
    ext_modules = [libduckdb]
)

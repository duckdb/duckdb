#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import platform

from setuptools import setup, Extension

from setup_utils import setup_data_files, get_scm_conf, get_setup_requires, get_extension_args

extensions = ['parquet', 'icu', 'fts', 'tpch', 'tpcds', 'visualizer']

if platform.system() == 'Windows':
    extensions = ['parquet', 'icu', 'fts']

libraries = []

# make sure we are in the right directory
os.chdir(os.path.dirname(os.path.realpath(__file__)))

extension_args = get_extension_args(extensions, libraries)
data_files = setup_data_files(extension_args['data_files'])
libduckdb = Extension('duckdb', **extension_args)

setup(
    name="duckdb",
    description='DuckDB embedded database',
    keywords='DuckDB Database SQL OLAP',
    url="https://www.duckdb.org",
    long_description='See here for an introduction: https://duckdb.org/docs/api/python',
    license='MIT',
    install_requires=[  # these version is still available for Python 2, newer ones aren't
        'numpy>=1.14'
    ],
    data_files=data_files,
    packages=['duckdb_query_graph'],
    include_package_data=True,
    setup_requires=get_setup_requires(),
    use_scm_version=get_scm_conf(),
    tests_require=['pytest'],
    classifiers=[
        'Topic :: Database :: Database Engines/Servers',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
    ],
    ext_modules=[libduckdb],
    maintainer="Hannes Muehleisen",
    maintainer_email="hannes@cwi.nl"
)

#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import subprocess
import shutil
import platform


import distutils.spawn
from setuptools import setup, Extension
from setuptools.command.sdist import sdist


# make sure we are in the right directory
os.chdir(os.path.dirname(os.path.realpath(__file__)))

# check if amalgamation exists
if os.path.isfile(os.path.join('..', '..', 'scripts', 'amalgamation.py')):
    prev_wd = os.getcwd()
    target_header = os.path.join(prev_wd, 'duckdb.hpp')
    target_source = os.path.join(prev_wd, 'duckdb.cpp')
    os.chdir(os.path.join('..', '..'))
    sys.path.append('scripts')
    import amalgamation
    amalgamation.generate_amalgamation(target_source, target_header)
    os.chdir(prev_wd)


toolchain_args = ['-std=c++11']
#toolchain_args = ['-std=c++11', '-Wall', '-O0', '-g']

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


libduckdb = Extension('duckdb',
    include_dirs=['.', get_numpy_include(), get_pybind_include(), get_pybind_include(user=True)],
    sources=['duckdb_python.cpp', 'duckdb.cpp'],
    extra_compile_args=toolchain_args,
    extra_link_args=toolchain_args,
    language='c++')

# Only include pytest-runner in setup_requires if we're invoking tests
if {'pytest', 'test', 'ptr'}.intersection(sys.argv):
    setup_requires = ['pytest-runner']
else:
    setup_requires = []

setup(
    name = "duckdb",
    description = 'DuckDB embedded database',
    keywords = 'DuckDB Database SQL OLAP',
    url="https://www.duckdb.org",
    long_description = '',
    install_requires=[ # these versions are still available for Python 2, newer ones aren't
         'numpy>=1.14',
         'pandas>=0.23',
    ],
    packages=['duckdb_query_graph'],
    include_package_data=True,
    setup_requires=setup_requires + ["setuptools_scm"] + ['pybind11>=2.4'],
    use_scm_version = {"root": "../..", "relative_to": __file__},
    tests_require=['pytest'],
    classifiers = [
        'Topic :: Database :: Database Engines/Servers',
        'Intended Audience :: Developers'
    ],
    ext_modules = [libduckdb],
    maintainer = "Hannes Muehleisen",
    maintainer_email = "hannes@cwi.nl"
)

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

toolchain_args = ['-std=c++11', '-g0']
if 'DUCKDEBUG' in os.environ:
    toolchain_args = ['-std=c++11', '-Wall', '-O0', '-g']

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


if len(existing_duckdb_dir) == 0:
    # no existing library supplied: compile everything from source
    libduckdb = Extension('duckdb',
        include_dirs=['.', get_numpy_include(), get_pybind_include(), get_pybind_include(user=True)],
        sources=['duckdb_python.cpp', 'duckdb.cpp', 'parquet-extension.cpp'],
        extra_compile_args=toolchain_args,
        extra_link_args=toolchain_args,
        language='c++')
else:
    # existing lib provided: link to it
    def find_library_recursive(search_dir, potential_libnames):
        flist = os.listdir(search_dir)
        for fname in flist:
            fpath = os.path.join(search_dir, fname)
            if os.path.isdir(fpath):
                entry = find_library_recursive(fpath, potential_libnames)
                if entry != None:
                    return entry
            elif os.path.isfile(fpath) and fname in potential_libnames:
                return search_dir
        return None

    def find_library(search_dir, libname, libnames, library_dirs):
        if libname == 'Threads::Threads':
            libnames += ['pthread']
            return
        libextensions = ['.a', '.lib']
        libprefixes = ['', 'lib']
        potential_libnames = []
        for ext in libextensions:
            for prefix in libprefixes:
                potential_libnames.append(prefix + libname + ext)
        libdir = find_library_recursive(existing_duckdb_dir, potential_libnames)

        libnames += [libname]
        library_dirs += [libdir]

    libnames = ['duckdb_static', 'parquet_extension', 'icu_extension']

    library_dirs = []
    library_dirs += [os.path.join(existing_duckdb_dir, 'src')]
    library_dirs += [os.path.join(existing_duckdb_dir, 'extension', 'parquet')]
    library_dirs += [os.path.join(existing_duckdb_dir, 'extension', 'icu')]

    for libname in libraries:
        find_library(existing_duckdb_dir, libname, libnames, library_dirs)

    print(libnames, library_dirs)

    libduckdb = Extension('duckdb',
        include_dirs=['.', get_numpy_include(), get_pybind_include(), get_pybind_include(user=True)],
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

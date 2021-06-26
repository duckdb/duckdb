#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import os.path as path
import platform
import sys

from setuptools import setup
from distutils.command.sdist import sdist as _sdist
from distutils import dir_util, log


# monkey patch sdist to copy our custom setup.py
class Sdist(_sdist):
    def make_release_tree(self, base_dir, files):
        self.mkpath(base_dir)
        dir_util.create_tree(base_dir, files, dry_run=self.dry_run)

        if hasattr(os, 'link'):
            link = 'hard'
            msg = "making hard links in %s..." % base_dir
        else:
            link = None
            msg = "copying files to %s..." % base_dir

        if not files:
            log.warn("no files to distribute -- empty manifest?")
        else:
            log.info(msg)
        for file in files:
            if not os.path.isfile(file):
                log.warn("'%s' not a regular file -- skipping", file)
            else:
                dest = os.path.join(base_dir, file)
                self.copy_file(file, dest, link=link)
        dest = os.path.join(base_dir, 'setup.py')
        self.copy_file('httpfs_package/setup.py', dest, link=link)
        self.distribution.metadata.write_pkg_info(base_dir)


# special hack to allow the initial `python setup.py *dist*` when setup_utils is in the parent dir
current_dir = path.dirname(path.abspath(__file__))
sys.path.insert(0, current_dir[:current_dir.rfind(path.sep)])
from setup_utils import setup_data_files, get_scm_conf, get_libduckdb, parallel_cpp_compile
sys.path.pop(0)

# speed up compilation with: -j = cpu_number() on non Windows machines
if os.name != 'nt':
    import distutils.ccompiler
    distutils.ccompiler.CCompiler.compile = parallel_cpp_compile

extensions = ['parquet', 'icu', 'fts', 'tpch', 'tpcds', 'visualizer', 'httpfs']

if platform.system() == 'Windows':
    extensions = ['parquet', 'icu', 'fts']

# httpfs requires openssl libs
libraries = ['ssl', 'crypto']

# make sure we are in the right directory
os.chdir(os.path.dirname(os.path.realpath(os.path.join('..', __file__))))

libduckdb, extra_files, header_files, setup_requires = get_libduckdb(extensions, libraries)
data_files = setup_data_files(extra_files + header_files)

setup(
    name="duckdb-httpfs",
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
    setup_requires=setup_requires,
    use_scm_version=get_scm_conf(),
    tests_require=['pytest'],
    cmdclass={'sdist': Sdist},
    classifiers=[
        'Topic :: Database :: Database Engines/Servers',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
    ],
    ext_modules=[libduckdb],
    maintainer="Hannes Muehleisen",
    maintainer_email="hannes@cwi.nl"
)

#!/bin/sh

rm -rf .eggs .pytest_cache build dist duckdb.egg-info duckdb.cpp duckdb.hpp parquet-extension.cpp parquet-extension.hpp duckdb duckdb_tarball
rm -f sources.list includes.list githash.list
python3 -c "exec('try:\n\timport duckdb, shutil, os\n\tshutil.rmtree(duckdb.__file__.rsplit(os.path.sep, 1)[0])\nexcept:\n\texit(0);')"
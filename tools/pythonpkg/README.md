This is the DuckDB Python package

## Installing locally

Set the prefix path:

    export DUCKDB_PREFIX=/path/to/install/duckdb

Set the PYTHONPATH:

    export PYTHONPATH=${PYTHONPATH:+${PYTHONPATH}:}$(pip3 show six | \
      grep "Location:" | cut -d " " -f2 | \
      sed -e "s|/usr|${DUCKDB_PREFIX}|")

### Setup for development

Install the package from the root of the DuckDB reposity:

    cd ../..
    pip3 install --prefix $DUCKDB_PREFIX -e tools/pythonpkg

This creates a package that uses the files in `tools/pythonpkg`, the
best option during development.

### Setup for cloud storage

Alternatively, you may need the package files to reside under the same
prefix where the library is installed; e.g., when installing to cloud
storage from a notebook.

First, get the repository based version number and extract the source distribution.

    cd tools/pythonpkg
    export SETUPTOOLS_SCM_PRETEND_VERSION=$(python setup.py --version)
    python setup.py sdist
    cd ../..
 
Next, copy over the python package related files, and install the package.

    mkdir -p $DUCKDB_PREFIX/src/duckdb-pythonpkg
    tar --directory=$DUCKDB_PREFIX/src/duckdb-pythonpkg -xzpf tools/pythonpkg/dist/duckdb-${SETUPTOOLS_SCM_PRETEND_VERSION}.tar.gz
    pip3 install --prefix $DUCKDB_PREFIX -e $DUCKDB_PREFIX/src/duckdb-pythonpkg

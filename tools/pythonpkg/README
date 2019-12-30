This is the DuckDB Python package

## Installing locally

Set the prefix path:

    export DUCKDB_PREFIX=/path/to/install/duckdb

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

First, get the version number from the repository.

    cd tools/pythonpkg
    export DUCKDB_VERSION=$(python setup.py --version)
    sed -e "s/DUCKDB_VERSION/${DUCKDB_VERSION}.local/g" -i.orig setup.py
    cd ../..
 
Next, copy over the python package related files, and install the package.

    mkdir -p $DUCKDB_PREFIX/src/duckdb-pythonpkg
    cp -R tools/pythonpkg $DUCKDB_PREFIX/src/duckdb-pythonpkg
    pip3 install --prefix $DUCKDB_PREFIX -e $DUCKDB_PREFIX/src/duckdb-pythonpkg


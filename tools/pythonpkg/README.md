This is the DuckDB Python package

## Default installation

You would normally install the DuckDB released version using `pip` as follows:
    pip install duckdb

## Installing locally for development

For development, you may need a DuckDB python package that is installed from source. In order to install from source, the simplest way is by cloning the git repo, and running the make command with `BUILD_PYTHON` set:

    BUILD_PYTHON=1 make debug
    
Note that this will override any existing DuckDB installation you might have. You might also run into conflicts depending on your Python environment. In order to remedy that, it is possible to use virtualenv for installation, e.g. by running the following commands:

    virtualenv .venv --python=python3.8
    source .venv/bin/activate
    BUILD_PYTHON=1 make

You can also directly invoke the setup.py script from the `tools/pythonpkg` environment.

    cd tools/pythonpkg
    python3 setup.py install

Alternatively, using virtualenv and pip:

    # Create and activate Python virtualenv.
    virtualenv .venv
    source .venv/bin/activate

    # Compile and install DuckDB for Python.
    pip install -e tools/pythonpkg --verbose

This works fine for a single installation, but is not recommended for active development since incremental compilation does not always work correctly here.

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
    pip3 install --prefix $DUCKDB_PREFIX -e $DUCKDB_PREFIX/src/duckdb-pythonpkg/duckdb-${SETUPTOOLS_SCM_PRETEND_VERSION}

## Stubs

`*.pyi` stubs can be generated with [Mypy's `stubgen`](https://mypy.readthedocs.io/en/stable/stubgen.html).
There is a bit of a chicken and egg situation with this - the stubs should go in the package, but
`stubgen` needs to look at the package to generate the stubs!

Thus, the full process to generate new stubs and use the resultion package with them would look like:

    BUILD_PYTHON=1 make debug # installs package without / with old stubs
    make python-stubs
    BUILD_PYTHON=1 make debug # installs package with up-to-date stubs.
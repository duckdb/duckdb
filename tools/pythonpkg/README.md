# DuckDB Python package

## Default installation

You would normally install the DuckDB released version using pip as follows:

```bash
pip install duckdb
```

## Installing locally for development

For development, you may need a DuckDB Python package that is installed from source. Make sure you have the dependencies installed:

```bash
pip install -r requirements-dev.txt
```

### Installing with pip

In order to install from source, the simplest way is to install by cloning the Git repository and using pip:

```bash
cd tools/pythonpkg
python3 -m pip install .
```

To install in debug mode, set the environment variable `$DUCKDEBUG=1` (or some other non-zero value).

Note that this will override any existing DuckDB installation you might have. You might also run into conflicts depending on your Python environment. In order to remedy that, it is possible to use virtualenv for installation, e.g. by running the following commands:

```bash
cd tools/pythonpkg
virtualenv .venv --python=python3.12
source .venv/bin/activate
python3 -m pip install .
```

To test, run:

```bash
cd ../..
python3 -c "import duckdb; duckdb.sql('SELECT version() AS version').show()"
```

### Installing with make

You can build using the make command with `BUILD_PYTHON` flag set. For example:

```bash
BUILD_PYTHON=1 make debug
```

### Setup for cloud storage

Alternatively, you may need the package files to reside under the same
prefix where the library is installed; e.g., when installing to cloud
storage from a notebook.

First, get the repository based version number and extract the source distribution.

```bash
python3 -m pip install build # required for PEP 517 compliant source dists
cd tools/pythonpkg
export SETUPTOOLS_SCM_PRETEND_VERSION=$(python3 -m setuptools_scm)
pyproject-build . --sdist
cd ../..
```

Next, copy over the python package related files, and install the package.

```bash
mkdir -p $DUCKDB_PREFIX/src/duckdb-pythonpkg
tar --directory=$DUCKDB_PREFIX/src/duckdb-pythonpkg -xzpf tools/pythonpkg/dist/duckdb-${SETUPTOOLS_SCM_PRETEND_VERSION}.tar.gz
pip install --prefix $DUCKDB_PREFIX -e $DUCKDB_PREFIX/src/duckdb-pythonpkg/duckdb-${SETUPTOOLS_SCM_PRETEND_VERSION}
```

## Development and Stubs

`*.pyi` stubs in `duckdb-stubs` are manually maintained. The connection-related stubs are generated using dedicated scripts in `tools/pythonpkg/scripts/`:
- `generate_connection_stubs.py`
- `generate_connection_wrapper_stubs.py`

These stubs are important for autocomplete in many IDEs, as static-analysis based language servers can't introspect `duckdb`'s binary module.

To verify the stubs match the actual implementation:
```bash
python3 -m pytest tests/stubs
```

If you add new methods to the DuckDB Python API, you'll need to manually add corresponding type hints to the stub files.

## Frequently encountered issue with extensions

If you are faced with an error on `import duckdb`:

```console
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
  File "/usr/bin/python3/site-packages/duckdb/__init__.py", line 4, in <module>
    import duckdb.functional as functional
  File "/usr/bin/python3/site-packages/duckdb/functional/__init__.py", line 1, in <module>
    from duckdb.duckdb.functional import (
ImportError: dlopen(/usr/bin/python3/site-packages/duckdb/duckdb.cpython-311-darwin.so, 0x0002): symbol not found in flat namespace '_MD5_Final'
```

When building DuckDB it outputs which extensions are linked into DuckDB, the python package does not deal with linked in extensions very well.
The output looks something like this:
`-- Extensions linked into DuckDB: [json, fts, tpcds, tpch, parquet, icu, httpfs]`

`httpfs` should not be in that list, among others.
Refer to `extension/extension_config_local.cmake` or the other `*.cmake` files and make sure you add DONT_LINK to the problematic extension.
`tools/pythonpkg/duckdb_extension_config.cmake` contains the default list of extensions built for the python package
Anything that is linked that is not listed there should be considered problematic.

## Clang-tidy and CMakeLists

The pythonpkg does not use the CMakeLists for compilation, for that it uses pip and `package_build.py` mostly.
But we still have CMakeLists in the pythonpkg, for tidy-check and intellisense purposes.
For this reason it might not be instantly apparent that the CMakeLists are incorrectly set up, and will only result in a very confusing CI failure of TidyCheck.

To prevent this, or to help you when you encounter said CI failure, here are a couple of things to note about the CMakeLists in here.

The pythonpkg depends on `PythonLibs`, and `pybind11`, for some reason `PythonLibs` can not be found by clang-tidy when generating the `compile_commands.json` file
So the reason for clang-tidy failing is likely that there is no entry for a file in the `compile_commands.json`, check the CMakeLists to see why cmake did not register it.

Helpful information:
`clang-tidy` is not a standard binary on MacOS, and can not be installed with brew directly (doing so will try to install clang-format, and they are not the same thing)
Instead clang-tidy is part of `llvm`, so you'll need to install that (`brew install llvm`), after installing llvm you'll likely have to add the llvm binaries folder to your PATH variable to use clang-tidy
For example:

```bash
export PATH="$PATH:/opt/homebrew/Cellar/llvm/15.0.2/bin"
```

# What are py::objects and a py::handles??

These are classes provided by pybind11, the library we use to manage our interaction with the python environment.
py::handle is a direct wrapper around a raw PyObject* and does not manage any references.
py::object is similar to py::handle but it can handle refcounts.

I say *can* because it doesn't have to, using `py::reinterpret_borrow<py::object>(...)` we can create a non-owning py::object, this is essentially just a py::handle but py::handle can't be used if the prototype requires a py::object.

`py::reinterpret_steal<py::object>(...)` creates an owning py::object, this will increase the refcount of the python object and will decrease the refcount when the py::object goes out of scope.

When directly interacting with python functions that return a `PyObject*`, such as `PyDateTime_DATE_GET_TZINFO`, you should generally wrap the call in `py::reinterpret_steal` to take ownership of the returned object.

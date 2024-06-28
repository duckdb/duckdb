This is the DuckDB Python package

## Default installation

You would normally install the DuckDB released version using pip as follows:

    pip3 install duckdb

## Installing locally for development

For development, you may need a DuckDB Python package that is installed from source. Make sure you have the dependencies installed:

    pip3 install -r requirements-dev.txt

 In order to install from source, the simplest way is to install by cloning the git repo, and running the make command with `BUILD_PYTHON` set:

    BUILD_PYTHON=1 make debug

Note that this will override any existing DuckDB installation you might have. You might also run into conflicts depending on your Python environment. In order to remedy that, it is possible to use virtualenv for installation, e.g. by running the following commands:

    virtualenv .venv --python=python3.8
    source .venv/bin/activate
    BUILD_PYTHON=1 make

You can also directly invoke pip from the `tools/pythonpkg` environment.

    cd tools/pythonpkg
    python3 -m pip install .

Alternatively, using virtualenv and pip:

    # Create and activate Python virtualenv.
    virtualenv .venv
    source .venv/bin/activate

    # Compile and install DuckDB for Python.
    pip install -e tools/pythonpkg --verbose

This works fine for a single installation, but is not recommended for active development since incremental compilation does not always work correctly here.

If you are installing via `pip`, you can set the environment variable
`$DUCKDEBUG=1` (or some other non-zero value) to compile in debug mode.

### Setup for cloud storage

Alternatively, you may need the package files to reside under the same
prefix where the library is installed; e.g., when installing to cloud
storage from a notebook.

First, get the repository based version number and extract the source distribution.

    python3 -m pip install build # required for pep517 compliant source dists
    cd tools/pythonpkg
    export SETUPTOOLS_SCM_PRETEND_VERSION=$(python3 -m setuptools_scm)
    pyproject-build . --sdist
    cd ../..

Next, copy over the python package related files, and install the package.

    mkdir -p $DUCKDB_PREFIX/src/duckdb-pythonpkg
    tar --directory=$DUCKDB_PREFIX/src/duckdb-pythonpkg -xzpf tools/pythonpkg/dist/duckdb-${SETUPTOOLS_SCM_PRETEND_VERSION}.tar.gz
    pip3 install --prefix $DUCKDB_PREFIX -e $DUCKDB_PREFIX/src/duckdb-pythonpkg/duckdb-${SETUPTOOLS_SCM_PRETEND_VERSION}

## Development and Stubs

`*.pyi` stubs are generated with [Mypy's `stubgen`](https://mypy.readthedocs.io/en/stable/stubgen.html) and tweaked. These are important for autocomplete in many IDEs, as static-analysis based language servers can't introspect `duckdb`'s binary module.

The stubs from stubgen are pretty good, but not perfect. In some cases, you can help stubgen out: for example, function annotation types that it can't figure out should be specified in the cpp where necessary, as in the example
```cpp
// without this change, the generated stub is
// def query_df(self, df: object, virtual_table_name: str, sql_query: str) -> DuckDBPyRelation: ...
pybind_opts.disable_function_signatures();
m.def("query_df", &DuckDBPyRelation::QueryDF,
      "query_df(self, df: pandas.DataFrame, virtual_table_name: str, sql_query: str) -> DuckDBPyRelation \n"
      "Run the given SQL query in sql_query on the view named virtual_table_name that contains the content of "
      "Data.Frame df",
      py::arg("df"), py::arg("virtual_table_name"), py::arg("sql_query"));
pybind_opts.enable_function_signatures();
// now the generated stub is
// def query_df(self, df: pandas.DataFrame, virtual_table_name: str, sql_query: str) -> DuckDBPyRelation: ...
```

If you want to regenerate the stubs, there is a bit of a chicken and egg situation - the stubs should go in the package, but
`stubgen` needs to look at the package to generate the stubs!

There is a test that you can run to check the stubs match the real duckdb package - this runs in CI (sorry in advance...). If you add a method to the duckdb py library and forget to add it to the stub, this test will helpfully fail. The test is a run of [mypy.stubtest](https://github.com/python/mypy/issues/5028#issuecomment-740101546).

The workflow for getting the stubs right will look something like

```sh
# Edit python package...
vim tools/pythonpkg/duckdb_python.cpp # or whatever

# Install duckdb python package to
#  - compile it so stubgen can read it
#  - put the stubs in editable mode so you can tweak them easily
(cd tools/pythonpkg; pip install -e .)
# regerate stub once your changes have been installed.
scripts/regenerate_python_stubs.sh
# (re-apply our fixes on top of generate stubs,
# hint: git add -p; git checkout HEAD tools/pythonpkg/duckdb-stubs)

# check tests
pytest tests/stubs
# edit and re-test stubs until you're happy
```

All the above should be done in a virtualenv.

## Frequently encountered issue with extensions:

If you are faced with an error on `import duckdb`:
```
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

<img src="https://raw.githubusercontent.com/apache/datasketches-website/master/logos/svg/datasketches-HorizontalColor-TM.svg" width="75%" alt="Apache DataSketchs Logo">

# The Apache DataSketches Library for Python

This is the official version of the [Apache DataSketches](https://datasketches.apache.org) Python library.

In the analysis of big data there are often problem queries that don’t scale because they require huge compute resources and time to generate exact results. Examples include count distinct, quantiles, most-frequent items, joins, matrix computations, and graph analysis.

If approximate results are acceptable, there is a class of specialized algorithms, called streaming algorithms, or sketches that can produce results orders-of magnitude faster and with mathematically proven error bounds. For interactive queries there may not be other viable alternatives, and in the case of real-time analysis, sketches are the only known solution.

This package provides a variety of sketches as described below. Wherever a specific type of sketch exists in Apache DataSketches packages for other languages, the sketches will be portable between languages (for platforms with the same endianness).

## Building and Installation

Once cloned, the library can be installed by running `python3 -m pip install .` in the project root directory -- not the python subdirectory -- which will also install the necessary dependencies, namely numpy and [pybind11[global]](https://github.com/pybind/pybind11).

If you prefer to call the `setup.py` build script directly, which is discoraged, you must first install `pybind11[global]`, as well as any other dependencies listed under the build-system section in `pyproject.toml`.

The library is also available from PyPI via `python3 -m pip install datasketches`.

## Usage

Having installed the library, loading the Apache Datasketches Library in Python is simple: `import datasketches`.

The unit tests are mostly structured in a tutorial style and can be used as a reference example for how to feed data into and query the different types of sketches.

## Available Sketch Classes

- KLL (Absolute Error Quantiles)
  - `kll_ints_sketch`
  - `kll_floats_sketch`
  - `kll_doubles_sketch`
- Quantiles (Absolute Error Quantiles, inferior algorithm)
  - `quantiles_ints_sketch`
  - `quantiles_floats_sketch`
  - `quantiles_doubles_sketch`
- REQ (Relative Error Quantiles)
  - `req_ints_sketch`
  - `req_floats_sketch`
- Frequent Items
  - `frequent_strings_sketch`
  - Error types are `frequent_items_error_type.{NO_FALSE_NEGATIVES | NO_FALSE_POSITIVES}`
- Theta
  - `update_theta_sketch`
  - `compact_theta_sketch` (cannot be instantiated directly)
  - `theta_union`
  - `theta_intersection`
  - `theta_a_not_b`
- HLL
  - `hll_sketch`
  - `hll_union`
  - Target HLL types are `tgt_hll_type.{HLL_4 | HLL_6 | HLL_8}`
- CPC
  - `cpc_sketch`
  - `cpc_union`
- VarOpt Sampling
  - `var_opt_sketch`
  - `var_opt_union`
- Vector of KLL
  - `vector_of_kll_ints_sketches`
  - `vector_of_kll_floats_sketches`
- Kolmogorov-Smirnov Test
  - `ks_test` applied to a pair of matched-type Absolute Error quantiles sketches

## Known Differences from C++

The Python API largely mirrors the C++ API, with a few minor exceptions: The primary known differences are that Python on modern platforms does not support unsigned integer values or numeric values with fewer than 64 bits. As a result, you may not be able to produce identical sketches from within Python as you can with Java and C++. Loading those sketches after they have been serialized from another language will work as expected.

The Vector of KLL object is currently exclusive to python, and holds an array of independent KLL sketches. This is useful for creating a set of KLL sketches over a vector and has been designed to allow input as either a vector or a matrix of multiple vectors.

We have also removed reliance on a builder class for theta sketches as Python allows named arguments to the constructor, not strictly positional arguments.

## Developer Instructions

The only developer-specific instructions relate to running unit tests.

### Unit tests

The Python unit tests are run via `tox`, with no arguments, from the project root directory -- not the python subdirectory. Tox creates a temporary virtual environment in which to build and run teh unit tests. In the event you are missing the necessary pacakge, tox may be installed with `python3 -m pip install --upgrade tox`.

## License

The Apache DataSketches Library is distrubted under an Apache 2.0 License.

There may be precompiled binaries provided as a convenience and distributed through PyPI via [https://pypi.org/project/datasketches/] contain compiled code from [pybind11](https://github.com/pybind/pybind11), which is distributed under a BSD license.

#include "Python.h"

#define NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION // motherfucker

#include <numpy/arrayobject.h>
#include <numpy/npy_common.h>
#include <duckdb.hpp>

PyObject *PyMaskedArray_FromCol(duckdb::MaterializedQueryResult *result, size_t col_idx);

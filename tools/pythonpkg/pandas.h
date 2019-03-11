#include "Python.h"
#include "duckdb.h"

#define NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION // motherfucker

#include <numpy/arrayobject.h>
#include <numpy/npy_common.h>

PyObject *PyNullMask_FromCol(duckdb_result *result, size_t col_idx);
PyObject *PyMaskedArray_FromCol(duckdb_result *result, size_t col_idx, char **return_message);
PyObject *PyArrayObject_FromCol(duckdb_result *result, size_t col_idx, char **return_message);

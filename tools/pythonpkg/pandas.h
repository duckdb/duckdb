#include "Python.h"
#include "duckdb.h"

#define NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION // motherfucker

#include <numpy/arrayobject.h>
#include <numpy/npy_common.h>

PyObject *PyNullMask_FromCol(duckdb_column *col, size_t t_start, size_t t_end);
PyObject *PyMaskedArray_FromCol(duckdb_column *col, size_t t_start, size_t t_end, char **return_message, bool copy);
PyObject *PyArrayObject_FromCol(duckdb_column *col, size_t t_start, size_t t_end, char **return_message, bool copy);

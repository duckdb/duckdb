#include "pandas.h"

static void *duckdb_pandas_init() {
	if (PyArray_API == NULL) {
		import_array();
	}
	return NULL;
}

PyObject *PyNullMask_FromCol(duckdb_column *col, size_t t_start, size_t t_end) {
	// We will now construct the Masked array, we start by setting everything to False
	size_t count = t_end - t_start;
	npy_intp elements[1] = {count};
	PyArrayObject *nullmask = (PyArrayObject *)PyArray_EMPTY(1, elements, NPY_BOOL, 0);

	bool found_nil = false;
	bool *mask_data = (bool *)PyArray_DATA(nullmask);

	for (size_t row_idx = 0; row_idx < count; row_idx++) {
		mask_data[row_idx] = duckdb_value_is_null(*col, row_idx);
		found_nil = found_nil || mask_data[row_idx];
	}

	if (!found_nil) {
		Py_DECREF(nullmask);
		Py_RETURN_NONE;
	}

	return (PyObject *)nullmask;
}

PyObject *PyMaskedArray_FromCol(duckdb_column *col, size_t t_start, size_t t_end, char **return_message) {
	char *msg = NULL;
	PyObject *vararray;
	duckdb_pandas_init();

	vararray = PyArrayObject_FromCol(col, t_start, t_end, return_message);
	if (vararray == NULL) {
		return NULL;
	}

	// To deal with null values, we use the numpy masked array structure
	// The masked array structure is an object with two arrays of equal size, a data array and a mask array
	// The mask array is a boolean array that has the value 'True' when the element is NULL, and 'False' otherwise
	PyObject *mask;
	PyObject *mafunc = PyObject_GetAttrString(PyImport_Import(PyUnicode_FromString("numpy.ma")), "masked_array");
	PyObject *maargs;
	PyObject *nullmask = PyNullMask_FromCol(col, t_start, t_end);

	if (nullmask == Py_None) {
		maargs = PyTuple_New(1);
		PyTuple_SetItem(maargs, 0, vararray);
	} else {
		maargs = PyTuple_New(2);
		PyTuple_SetItem(maargs, 0, vararray);
		PyTuple_SetItem(maargs, 1, (PyObject *)nullmask);
	}

	// Now we will actually construct the mask by calling the masked array constructor
	mask = PyObject_CallObject(mafunc, maargs);
	if (!mask) {
		// msg = createException(MAL, "pyapi.eval", "Failed to create mask");
		goto wrapup;
	}
	Py_DECREF(maargs);
	Py_DECREF(mafunc);

	vararray = mask;

	return vararray;
wrapup:
	*return_message = msg;
	return NULL;
}

#define BAT_TO_NP(bat, mtpe, nptpe)                                                                                    \
	{                                                                                                                  \
		vararray = PyArray_EMPTY(1, elements, nptpe, 0);                                                               \
		memcpy(PyArray_DATA((PyArrayObject *)vararray), col->data, sizeof(mtpe) * (t_end - t_start));                  \
	}

PyObject *PyArrayObject_FromCol(duckdb_column *col, size_t t_start, size_t t_end, char **return_message) {
	// This variable will hold the converted Python object
	PyObject *vararray = NULL;
	char *msg = NULL;
	npy_intp elements[1] = {t_end - t_start};
	duckdb_pandas_init();

	switch (col->type) {

	case DUCKDB_TYPE_BOOLEAN:
	case DUCKDB_TYPE_TINYINT:
		BAT_TO_NP(b, int8_t, NPY_INT8);
		break;

	case DUCKDB_TYPE_SMALLINT:
		BAT_TO_NP(b, int16_t, NPY_INT16);
		break;

	case DUCKDB_TYPE_INTEGER:
		BAT_TO_NP(b, int32_t, NPY_INT32);
		break;

	case DUCKDB_TYPE_BIGINT:
		BAT_TO_NP(b, int64_t, NPY_INT64);
		break;

	case DUCKDB_TYPE_DECIMAL:
		BAT_TO_NP(b, double, NPY_FLOAT64);
		break;

	case DUCKDB_TYPE_VARCHAR: {
		// create a NPY_OBJECT array object
		// TODO convert string heap separately
		vararray = PyArray_New(&PyArray_Type, 1, elements, NPY_OBJECT, NULL, NULL, 0, 0, NULL);

		{
			PyObject **data = ((PyObject **)PyArray_DATA((PyArrayObject *)vararray));
			PyObject *obj;
			for (size_t row_idx = 0; row_idx < col->count; row_idx++) {
				char *t = ((char **)col->data)[row_idx];
				if (duckdb_value_is_null(*col, row_idx)) {
					obj = PyUnicode_FromString("-");
				} else {
					// TODO this is going to be sloooow
					obj = PyUnicode_FromString(t);
				}
				if (obj == NULL) {
					// TODO COMPLAIN
					goto wrapup;
				}
				data[row_idx] = obj;
			}
		}

	} break;

	default:
		// TODO ERROR
		goto wrapup;
	}

	if (vararray == NULL) {
		//	msg = createException(MAL, "pyapi.eval", "Failed to convert BAT to Numpy array.");
		goto wrapup;
	}
	return vararray;
wrapup:
	*return_message = msg;
	return NULL;
}

#include "pandas.h"

typedef struct {
	PyObject *array;
	PyObject *nullmask;
} duckdb_numpy_result;

static void *duckdb_pandas_init() {
	if (PyArray_API == NULL) {
		import_array();
	}
	return NULL;
}

static uint8_t duckdb_type_to_numpy_type(duckdb::TypeId type) {
	switch (type) {
		case duckdb::TypeId::BOOLEAN:
		case duckdb::TypeId::TINYINT:
			return NPY_INT8;
		case duckdb::TypeId::SMALLINT:
			return NPY_INT16;
		case duckdb::TypeId::INTEGER:
			return NPY_INT32;
		case duckdb::TypeId::BIGINT:
			return NPY_INT64;
		case duckdb::TypeId::FLOAT:
			return NPY_FLOAT32;
		case duckdb::TypeId::DOUBLE:
			return NPY_FLOAT64;
		case duckdb::TypeId::VARCHAR:
			return NPY_OBJECT;
		default:
			assert(0);
	}
	return 0;
}

static bool duckdb_col_to_numpy(duckdb::MaterializedQueryResult *result, size_t col_idx, duckdb_numpy_result* res) {
	// TODO assert that the type lengths are indeed compatible
	npy_intp dims[1] = {static_cast<npy_intp>(result->collection.count)};

	res->array = PyArray_EMPTY(1, dims, duckdb_type_to_numpy_type(result->types[col_idx]), 0);
	res->nullmask = PyArray_EMPTY(1, dims, NPY_BOOL, 0);
	if (!res->array || !res->nullmask) {
		return false;
	}

	size_t offset = 0;
	bool found_nil = false;
	bool* mask_data = (bool *)PyArray_DATA((PyArrayObject *)res->nullmask);
	char* array_data = (char*) PyArray_DATA((PyArrayObject *)res->array);

	auto duckdb_type = result->types[col_idx];
	auto duckdb_type_size =  duckdb::GetTypeIdSize(duckdb_type);

	while(true) {
		auto chunk = result->Fetch();
		if (chunk->size() == 0) break;
		switch(result->types[col_idx]) {
		case duckdb::TypeId::VARCHAR:
			for (size_t chunk_idx = 0; chunk_idx < chunk->size(); chunk_idx++) {
				PyObject *str_obj = nullptr;
				if (!chunk->data[col_idx].nullmask[chunk_idx]) {
					str_obj = PyUnicode_FromString(((const char**) chunk->data[col_idx].data)[chunk_idx]);
				}
				((PyObject**) array_data)[offset + chunk_idx] = str_obj;
			}
			break;
		default:
			assert(duckdb::TypeIsConstantSize(duckdb_type));
			memcpy(array_data + (offset * duckdb_type_size), chunk->data[col_idx].data, duckdb_type_size * chunk->size());
		}

		for (size_t chunk_idx = 0; chunk_idx < chunk->size(); chunk_idx++) {
			mask_data[chunk_idx + offset] = chunk->data[col_idx].nullmask[chunk_idx];
			found_nil = found_nil || mask_data[chunk_idx + offset];
		}
		offset += chunk->size();
	}
	if (!found_nil) { // we don't need no nullmask
		Py_DECREF(res->nullmask);
		res->nullmask = Py_None;
	}

	return true;
}


PyObject *PyMaskedArray_FromCol(duckdb::MaterializedQueryResult *result, size_t col_idx) {
	duckdb_pandas_init();
	PyObject *mafunc = PyObject_GetAttrString(PyImport_Import(PyUnicode_FromString("numpy.ma")), "masked_array");
	assert(mafunc);

	duckdb_numpy_result res = {nullptr, nullptr};
	bool convert_success = duckdb_col_to_numpy(result, col_idx, &res);

	if (!convert_success) {
		goto wrapup;
	}

	// To deal with null values, we use the numpy masked array structure
	// The masked array structure is an object with two arrays of equal size, a data array and a mask array
	// The mask array is a boolean array that has the value 'True' when the element is NULL, and 'False' otherwise
	PyObject *mask;
	PyObject *maargs;

	if (res.nullmask == Py_None) {
		maargs = PyTuple_New(1);
		PyTuple_SetItem(maargs, 0, res.array);
	} else {
		maargs = PyTuple_New(2);
		PyTuple_SetItem(maargs, 0, res.array);
		PyTuple_SetItem(maargs, 1, res.nullmask);
	}

	// Now we will actually construct the mask by calling the masked array constructor
	mask = PyObject_CallObject(mafunc, maargs);
	if (!mask) {
		goto wrapup;
	}
	Py_DECREF(maargs);
	Py_DECREF(mafunc);

	return mask;
wrapup:
	return NULL;
}


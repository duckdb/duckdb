#include "cursor.h"
#include <vector>
#include "module.h"

#define NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION // motherfucker

#include <numpy/arrayobject.h>
#include <numpy/npy_common.h>

PyObject *duckdb_cursor_iternext(duckdb_Cursor *self);

static const char errmsg_fetch_across_rollback[] =
    "Cursor needed to be reset because of commit/rollback and can no longer be fetched from.";

static int duckdb_cursor_init(duckdb_Cursor *self, PyObject *args, PyObject *kwargs) {
	duckdb_Connection *connection;

	if (!PyArg_ParseTuple(args, "O!", &duckdb_ConnectionType, &connection)) {
		return -1;
	}

	Py_INCREF(connection);
	// unclear but works
	Py_XSETREF(self->connection, connection);
	self->closed = 0;
	self->reset = 0;
	self->rowcount = -1L;

	self->initialized = 1;

	return 0;
}

static void duckdb_cursor_dealloc(duckdb_Cursor *self) {
	/* Reset the statement if the user has not closed the cursor */
	duckdb_cursor_close(self, NULL);

	Py_XDECREF(self->connection);
	Py_TYPE(self)->tp_free((PyObject *)self);
}

/*
 * Checks if a cursor object is usable.
 *
 * 0 => error; 1 => ok
 */
static int check_cursor(duckdb_Cursor *cur) {
	if (!cur->initialized) {
		PyErr_SetString(duckdb_DatabaseError, "Base Cursor.__init__ not called.");
		return 0;
	}

	if (cur->closed) {
		PyErr_SetString(duckdb_DatabaseError, "Cannot operate on a closed cursor.");
		return 0;
	}

	return duckdb_check_connection(cur->connection);
}

static const char *_duckdb_stringconvert(PyObject *pystr) {
	const char *cstr;
#if PY_MAJOR_VERSION >= 3
	assert(PyUnicode_Check(pystr));
	Py_ssize_t cstr_len;
	cstr = PyUnicode_AsUTF8AndSize(pystr, &cstr_len);

	if (cstr == NULL) {
		throw std::runtime_error("Got NULL pointer from string object");
	}
	if (strlen(cstr) != (size_t)cstr_len) {
		throw std::runtime_error("String object has embedded NULL");
	}
#else
	if (PyString_Check(pystr)) {
		cstr = PyString_AsString(pystr);
	} else {
		auto bytestr = PyUnicode_AsUTF8String(pystr);
		if (!bytestr) {
			throw std::runtime_error("Can't convert string object to unicode");
		}
		cstr = PyString_AsString(bytestr);
	}
	if (cstr == NULL) {
		throw std::runtime_error("Got NULL pointer from string object");
	}
#endif
	return cstr;
}

typedef enum { TYPE_INT, TYPE_LONG, TYPE_FLOAT, TYPE_STRING, TYPE_BUFFER, TYPE_UNKNOWN } parameter_type;

#ifdef WORDS_BIGENDIAN
#define IS_LITTLE_ENDIAN 0
#else
#define IS_LITTLE_ENDIAN 1
#endif

duckdb::Value _duckdb_pyobject_to_value(PyObject *parameter) {
	parameter_type paramtype = TYPE_UNKNOWN;

	if (parameter == Py_None) {
		return duckdb::Value();
	}
#if PY_MAJOR_VERSION < 3
	if (PyInt_CheckExact(parameter)) {
		paramtype = TYPE_INT;
	} else
#endif
	    if (PyLong_CheckExact(parameter)) {
		paramtype = TYPE_LONG;
	} else if (PyFloat_CheckExact(parameter)) {
		paramtype = TYPE_FLOAT;
#if PY_MAJOR_VERSION >= 3
	} else if (PyUnicode_CheckExact(parameter)) {
#else
	} else if (PyString_CheckExact(parameter)) {
#endif
		paramtype = TYPE_STRING;
	} else if (PyLong_Check(parameter)) {
		paramtype = TYPE_LONG;
	} else if (PyFloat_Check(parameter)) {
		paramtype = TYPE_FLOAT;
	} else if (PyUnicode_Check(parameter)) {
		paramtype = TYPE_STRING;
	} else if (PyObject_CheckBuffer(parameter)) {
		paramtype = TYPE_BUFFER;
	} else {
		paramtype = TYPE_UNKNOWN;
	}

	switch (paramtype) {
#if PY_MAJOR_VERSION < 3
	case TYPE_INT: {
		return duckdb::Value::BIGINT(PyInt_AsLong(parameter));
	}
#endif
	case TYPE_LONG: {
		int overflow;
		int64_t value = PyLong_AsLongLongAndOverflow(parameter, &overflow);
		if (overflow) {
			throw std::runtime_error("Overflow in long object");
		}
		return duckdb::Value::BIGINT(value);
	}
	case TYPE_FLOAT:
		return duckdb::Value::DOUBLE(PyFloat_AsDouble(parameter));
	case TYPE_STRING:
		return duckdb::Value(_duckdb_stringconvert(parameter));
	default:
		break;
	}
	// TODO complain
	throw std::runtime_error("Failed to convert object");
}

// this parameter binding mess is inherited from pysqlite

void _duckdb_bind_parameters(PyObject *parameters, std::vector<duckdb::Value> &params) {
	PyObject *current_param;
	int i;
	int num_params_needed = params.size();
	Py_ssize_t num_params = 0;

	if (PyTuple_CheckExact(parameters) || PyList_CheckExact(parameters) ||
	    (!PyDict_Check(parameters) && PySequence_Check(parameters))) {
		/* parameters passed as sequence */
		if (PyTuple_CheckExact(parameters)) {
			num_params = PyTuple_GET_SIZE(parameters);
		} else if (PyList_CheckExact(parameters)) {
			num_params = PyList_GET_SIZE(parameters);
		} else {
			num_params = PySequence_Size(parameters);
		}
		if (num_params != num_params_needed) {
			PyErr_Format(duckdb_DatabaseError,
			             "Incorrect number of bindings supplied. The current "
			             "statement uses %d, and there are %zd supplied.",
			             num_params_needed, num_params);
			return;
		}
		for (i = 0; i < num_params; i++) {
			if (PyTuple_CheckExact(parameters)) {
				current_param = PyTuple_GET_ITEM(parameters, i);
				Py_XINCREF(current_param);
			} else if (PyList_CheckExact(parameters)) {
				current_param = PyList_GET_ITEM(parameters, i);
				Py_XINCREF(current_param);
			} else {
				current_param = PySequence_GetItem(parameters, i);
			}
			if (!current_param) {
				return;
			}

			try {
				params[i] = _duckdb_pyobject_to_value(current_param);
			} catch (std::exception &e) {
				PyErr_Format(duckdb_DatabaseError, "Error binding parameter %d - %s", i, e.what());
				return;
			}
		}
	} else {
		PyErr_SetString(duckdb_DatabaseError, "parameters are of unsupported type");
	}
}

// many hatred
#if PY_MAJOR_VERSION >= 3
#define STRING_PARSE_FLAG "U"
#else
#define STRING_PARSE_FLAG "O"
#endif

// borrowed from the sqlite module
static PyObject *_duckdb_query_execute(duckdb_Cursor *self, int multiple, PyObject *args) {
	PyObject *operation;
	PyObject *parameters_list = NULL;
	PyObject *parameters_iter = NULL;
	PyObject *parameters = NULL;
	PyObject *second_argument = NULL;

	bool need_transaction;

	std::vector<duckdb::Value> params;
	std::unique_ptr<duckdb::PreparedStatement> prep;
	const char *sql_cstr;

	if (!check_cursor(self)) {
		goto error;
	}

	need_transaction = multiple && self->connection->conn->context->transaction.IsAutoCommit();

	if (need_transaction) {
		self->connection->conn->Query("BEGIN TRANSACTION");
	}

	// unify the various possible parameters from `args` into `parameters_iter`
	if (multiple) {
		/* executemany() */
		if (!PyArg_ParseTuple(args, STRING_PARSE_FLAG "O", &operation, &second_argument)) {
			goto error;
		}

		if (PyIter_Check(second_argument)) {
			/* iterator */
			Py_INCREF(second_argument);
			parameters_iter = second_argument;
		} else {
			/* sequence */
			parameters_iter = PyObject_GetIter(second_argument);
			if (!parameters_iter) {
				goto error;
			}
		}
	} else {
		/* execute() */
		if (!PyArg_ParseTuple(args, STRING_PARSE_FLAG "|O", &operation, &second_argument)) {
			goto error;
		}

		parameters_list = PyList_New(0);
		if (!parameters_list) {
			goto error;
		}

		if (second_argument == NULL) {
			second_argument = PyTuple_New(0); // push an empty tuple so we have an entry in parameters_list
			if (!second_argument) {
				goto error;
			}
		} else {
			Py_INCREF(second_argument);
		}
		if (PyList_Append(parameters_list, second_argument) != 0) {
			Py_DECREF(second_argument);
			goto error;
		}
		Py_DECREF(second_argument);

		parameters_iter = PyObject_GetIter(parameters_list);
		if (!parameters_iter) {
			goto error;
		}
	}
	self->rowcount = 0L;
	self->reset = 0;

	sql_cstr = _duckdb_stringconvert(operation);
	if (!sql_cstr) {
		PyErr_SetString(duckdb_DatabaseError, "Can't convert query to string");
		goto error;
	}

	prep = self->connection->conn->Prepare(sql_cstr);
	if (!prep->success) {
		PyErr_SetString(duckdb_DatabaseError, prep->error.c_str());
		goto error;
	}

	params.resize(prep->n_param);

	while (1) {
		parameters = PyIter_Next(parameters_iter);
		if (!parameters) {
			break;
		}

		// bind aprameters if any
		_duckdb_bind_parameters(parameters, params);
		if (PyErr_Occurred()) {
			goto error;
		}

		// fire!
		auto res = prep->Execute(params, false); // TODO we could do streaming results here
		if (!res->success) {
			PyErr_SetString(duckdb_DatabaseError, res->error.c_str());
			goto error;
		}
		assert(res->type == duckdb::QueryResultType::MATERIALIZED_RESULT);
		self->result = std::unique_ptr<duckdb::MaterializedQueryResult>(
		    static_cast<duckdb::MaterializedQueryResult *>(res.release()));
		Py_XDECREF(parameters);
		self->rowcount = self->result->collection.count;
		self->closed = 0;
		self->offset = 0;
	}

error:
	Py_XDECREF(parameters);
	Py_XDECREF(parameters_iter);
	Py_XDECREF(parameters_list);

	if (PyErr_Occurred()) {
		if (need_transaction) {
			self->connection->conn->Query("ROLLBACK");
		}
		self->rowcount = -1L;
		return NULL;
	} else {
		if (need_transaction) {
			self->connection->conn->Query("COMMIT");
		}
		Py_INCREF(self);
		return (PyObject *)self;
	}
}

PyObject *duckdb_cursor_execute(duckdb_Cursor *self, PyObject *args) {
	return _duckdb_query_execute(self, 0, args);
}

PyObject *duckdb_cursor_executemany(duckdb_Cursor *self, PyObject *args) {
	return _duckdb_query_execute(self, 1, args);
}

PyObject *duckdb_cursor_fetchone(duckdb_Cursor *self) {
	PyObject *row;

	row = duckdb_cursor_iternext(self);
	if (!row && !PyErr_Occurred()) {
		Py_RETURN_NONE;
	}

	return row;
}

PyObject *duckdb_cursor_fetchall(duckdb_Cursor *self) {
	PyObject *row;
	PyObject *list;

	list = PyList_New(0);
	if (!list) {
		return NULL;
	}

	/* just make sure we enter the loop */
	row = (PyObject *)Py_None;
	while (row) {
		row = duckdb_cursor_iternext(self);
		if (row) {
			PyList_Append(list, row);
			Py_DECREF(row);
		}
	}

	if (PyErr_Occurred()) {
		Py_DECREF(list);
		return NULL;
	} else {
		return list;
	}
}

static PyObject *fromdict_ref = NULL;
static PyObject *mafunc_ref = NULL;

static uint8_t duckdb_type_to_numpy_type(duckdb::TypeId type, duckdb::SQLTypeId sql_type) {
	switch (type) {
	case duckdb::TypeId::BOOL:
	case duckdb::TypeId::INT8:
		return NPY_INT8;
	case duckdb::TypeId::INT16:
		return NPY_INT16;
	case duckdb::TypeId::INT32:
		return NPY_INT32;
	case duckdb::TypeId::INT64:
		if (sql_type == duckdb::SQLTypeId::TIMESTAMP) {
			return NPY_DATETIME;
		} else {
			return NPY_INT64;
		}
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

typedef struct {
	PyObject *array = nullptr;
	PyObject *nullmask = nullptr;
	bool found_nil = false;
} duckdb_numpy_result;

PyObject *duckdb_cursor_fetchnumpy(duckdb_Cursor *self) {
	if (!check_cursor(self)) {
		return NULL;
	}

	auto result = self->result.get();
	assert(result);

	auto ncol = result->collection.column_count();
	auto nrow = result->collection.count;

	auto cols = new duckdb_numpy_result[ncol];
	npy_intp dims[1] = {static_cast<npy_intp>(nrow)};

	// step 1: allocate data and nullmasks for columns
	for (size_t col_idx = 0; col_idx < ncol; col_idx++) {

		// two owned references for each column, .array and .nullmask
		PyArray_Descr *descr =
		    PyArray_DescrNewFromType(duckdb_type_to_numpy_type(result->types[col_idx], result->sql_types[col_idx].id));
		// In the case of timestamps, we need to set the datetime64 unit explicitly.
		if (result->sql_types[col_idx].id == duckdb::SQLTypeId::TIMESTAMP) {
			auto dtype = reinterpret_cast<PyArray_DatetimeDTypeMetaData *>(descr->c_metadata);
			dtype->meta.base = NPY_FR_ms;
		}
		cols[col_idx].array = PyArray_Empty(1, dims, descr, 0);
		cols[col_idx].nullmask = PyArray_EMPTY(1, dims, NPY_BOOL, 0);
		if (!cols[col_idx].array || !cols[col_idx].nullmask) {
			PyErr_SetString(duckdb_DatabaseError, "memory allocation error");
			self->result = nullptr;
			return NULL;
		}
	}

	// step 2: fetch into the allocated arrays
	size_t offset = 0;
	while (true) {
		auto chunk = result->Fetch();
		if (chunk->size() == 0)
			break;
		for (size_t col_idx = 0; col_idx < ncol; col_idx++) {

			auto duckdb_type = result->types[col_idx];
			auto duckdb_type_size = duckdb::GetTypeIdSize(duckdb_type);

			char *array_data = (char *)PyArray_DATA((PyArrayObject *)cols[col_idx].array);
			bool *mask_data = (bool *)PyArray_DATA((PyArrayObject *)cols[col_idx].nullmask);

			// collect null mask into numpy array for masked arrays
			for (size_t chunk_idx = 0; chunk_idx < chunk->size(); chunk_idx++) {
				mask_data[chunk_idx + offset] = chunk->data[col_idx].nullmask[chunk_idx];
				cols[col_idx].found_nil = cols[col_idx].found_nil || mask_data[chunk_idx + offset];
			}

			switch (duckdb_type) {
			case duckdb::TypeId::VARCHAR:
				for (size_t chunk_idx = 0; chunk_idx < chunk->size(); chunk_idx++) {
					assert(!chunk->data[col_idx].sel_vector);
					PyObject *str_obj;
					if (!mask_data[chunk_idx + offset]) {
						str_obj = PyUnicode_FromString(((const char **)chunk->data[col_idx].GetData())[chunk_idx]);
					} else {
						assert(cols[col_idx].found_nil);
						str_obj = Py_None;
						Py_INCREF(str_obj);
					}
					((PyObject **)array_data)[offset + chunk_idx] = str_obj;
				}
				break;
			case duckdb::TypeId::INT64:
				if (result->sql_types[col_idx].id == duckdb::SQLTypeId::TIMESTAMP) {
					int64_t *array_data_ptr = reinterpret_cast<int64_t *>(array_data + (offset * duckdb_type_size));
					duckdb::timestamp_t *chunk_data_ptr = reinterpret_cast<int64_t *>(chunk->data[col_idx].GetData());
					for (size_t chunk_idx = 0; chunk_idx < chunk->size(); chunk_idx++) {
						// array_data_ptr[chunk_idx] = duckdb::Timestamp::GetEpoch(chunk_data_ptr[chunk_idx]) * 1000;
						auto timestamp = chunk_data_ptr[chunk_idx];
						array_data_ptr[chunk_idx] = duckdb::Date::Epoch(duckdb::Timestamp::GetDate(timestamp)) * 1000 +
						                            (int64_t)(duckdb::Timestamp::GetTime(timestamp));
					}
					break;
				}    // else fall-through-to-default
			default: // direct mapping types
				// TODO need to assert the types
				assert(duckdb::TypeIsConstantSize(duckdb_type));
				memcpy(array_data + (offset * duckdb_type_size), chunk->data[col_idx].GetData(),
				       duckdb_type_size * chunk->size());
			}
		}
		offset += chunk->size();
	}

	// step 4: convert to masked arrays
	PyObject *col_dict = PyDict_New();
	assert(mafunc_ref);

	for (size_t col_idx = 0; col_idx < ncol; col_idx++) {
		PyObject *mask;
		PyObject *maargs;

		// PyTuple_SetItem() is an exception and takes over ownership, hence no DECREF for arguments to it
		// https://docs.python.org/3/extending/extending.html#ownership-rules

		if (!cols[col_idx].found_nil) {
			maargs = PyTuple_New(1);
			PyTuple_SetItem(maargs, 0, cols[col_idx].array);
			Py_DECREF(cols[col_idx].nullmask);
		} else {
			maargs = PyTuple_New(2);
			PyTuple_SetItem(maargs, 0, cols[col_idx].array);
			PyTuple_SetItem(maargs, 1, cols[col_idx].nullmask);
		}

		// actually construct the mask by calling the masked array constructor
		mask = PyObject_CallObject(mafunc_ref, maargs);
		Py_DECREF(maargs);

		if (!mask) {
			PyErr_SetString(duckdb_DatabaseError, "unknown error");
			self->result = nullptr;
			return NULL;
		}
		auto name = PyUnicode_FromString(self->result->names[col_idx].c_str());
		PyDict_SetItem(col_dict, name, mask);
		Py_DECREF(name);
		Py_DECREF(mask);
	}
	// delete our holder object, the arrays within are either gone or we transferred ownership
	delete[] cols;
	self->result = nullptr;

	return col_dict;
}

PyObject *duckdb_cursor_fetchdf(duckdb_Cursor *self) {

	PyObject *res = PyObject_CallFunctionObjArgs(fromdict_ref, duckdb_cursor_fetchnumpy(self), NULL);
	if (!res) {
		return NULL;
	}
	Py_INCREF(res);
	return res;
}

PyObject *duckdb_cursor_iternext(duckdb_Cursor *self) {

	if (!check_cursor(self)) {
		return NULL;
	}

	if (self->offset >= self->rowcount) {
		return NULL;
	}
	if (self->reset) {
		PyErr_SetString(duckdb_DatabaseError, errmsg_fetch_across_rollback);
		return NULL;
	}

	auto ncol = self->result->collection.column_count();

	PyObject *row = PyList_New(ncol);

	//	DUCKDB_TYPE_TIMESTAMP,
	//	DUCKDB_TYPE_DATE,

	// FIXME actually switch on SQL types
	for (size_t col_idx = 0; col_idx < ncol; col_idx++) {
		PyObject *val = NULL;
		auto dval = self->result->collection.GetValue(col_idx, self->offset);

		if (dval.is_null) {
			PyList_SetItem(row, col_idx, Py_None);
			continue;
		}
		switch (dval.type) {
		case duckdb::TypeId::BOOL:
		case duckdb::TypeId::INT8:
			val = Py_BuildValue("b", dval.value_.tinyint);
			break;
		case duckdb::TypeId::INT16:
			val = Py_BuildValue("h", dval.value_.smallint);
			break;
		case duckdb::TypeId::INT32:
			val = Py_BuildValue("i", dval.value_.integer);
			break;
		case duckdb::TypeId::INT64:
			val = Py_BuildValue("L", dval.value_.bigint);
			break;
		case duckdb::TypeId::FLOAT:
			val = Py_BuildValue("f", dval.value_.float_);
			break;
		case duckdb::TypeId::DOUBLE:
			val = Py_BuildValue("d", dval.value_.double_);
			break;
		case duckdb::TypeId::VARCHAR:
			val = Py_BuildValue("s", dval.str_value.c_str());
			break;
		default:
			// TODO complain
			break;
		}
		if (val) {
			Py_INCREF(val);
			PyList_SetItem(row, col_idx, val);
			Py_DECREF(val);
		}
	}

	Py_INCREF(row);
	self->offset++;

	return row;
}

PyObject *duckdb_cursor_close(duckdb_Cursor *self, PyObject *args) {
	if (!self->connection) {
		PyErr_SetString(duckdb_DatabaseError, "Base Cursor.__init__ not called.");
		return NULL;
	}
	if (!duckdb_check_connection(self->connection)) {
		return NULL;
	}
	self->result = nullptr;

	self->closed = 1;
	self->rowcount = 0;
	self->offset = 0;

	Py_RETURN_NONE;
}

PyObject *duckdb_cursor_profile(duckdb_Cursor *self, PyObject *args) {
	int check = PyObject_IsTrue(args);
	if (check == -1) {
		PyErr_SetString(PyExc_TypeError,
		                "expected a boolean corresponding to either true (json) or false (query graph)");
		return NULL;
	}
	if (!self->connection) {
		PyErr_SetString(duckdb_DatabaseError, "Base Cursor.__init__ not called.");
		return NULL;
	}
	if (!duckdb_check_connection(self->connection)) {
		return NULL;
	}
	duckdb::ProfilerPrintFormat format =
	    check ? duckdb::ProfilerPrintFormat::JSON : duckdb::ProfilerPrintFormat::QUERY_TREE;
	return PyUnicode_FromString(self->connection->conn->GetProfilingInformation(format).c_str());
}

static PyMethodDef cursor_methods[] = {
    {"execute", (PyCFunction)duckdb_cursor_execute, METH_VARARGS, PyDoc_STR("Executes a SQL statement.")},
    {"executemany", (PyCFunction)duckdb_cursor_executemany, METH_VARARGS,
     PyDoc_STR("Repeatedly executes a SQL statement.")},
    {"fetchone", (PyCFunction)duckdb_cursor_fetchone, METH_NOARGS, PyDoc_STR("Fetches one row from the resultset.")},
    {"fetchall", (PyCFunction)duckdb_cursor_fetchall, METH_NOARGS, PyDoc_STR("Fetches all rows from the resultset.")},
    {"fetchnumpy", (PyCFunction)duckdb_cursor_fetchnumpy, METH_NOARGS,
     PyDoc_STR("Fetches all rows from the  resultset as a dict of numpy arrays.")},
    {"fetchdf", (PyCFunction)duckdb_cursor_fetchdf, METH_NOARGS,
     PyDoc_STR("Fetches all rows from the result set as a pandas DataFrame.")},
    {"close", (PyCFunction)duckdb_cursor_close, METH_NOARGS, PyDoc_STR("Closes the cursor.")},
    {"profile_info", (PyCFunction)duckdb_cursor_profile, METH_O,
     PyDoc_STR("Returns the profile information of the last running query.")},
    {NULL, NULL}};

static struct PyMemberDef cursor_members[] = {
    {"connection", T_OBJECT, offsetof(duckdb_Cursor, connection), READONLY},
    //    {"lastrowid", T_OBJECT, offsetof(pysqlite_Cursor, lastrowid), READONLY},
    {"rowcount", T_LONG, offsetof(duckdb_Cursor, rowcount), READONLY},
    {NULL}};

static const char cursor_doc[] = PyDoc_STR("DuckDB database cursor class.");

PyTypeObject duckdb_CursorType = {
    PyVarObject_HEAD_INIT(NULL, 0) "" MODULE_NAME ".Cursor", /* tp_name */
    sizeof(duckdb_Cursor),                                   /* tp_basicsize */
    0,                                                       /* tp_itemsize */
    (destructor)duckdb_cursor_dealloc,                       /* tp_dealloc */
    0,                                                       /* tp_print */
    0,                                                       /* tp_getattr */
    0,                                                       /* tp_setattr */
    0,                                                       /* tp_reserved */
    0,                                                       /* tp_repr */
    0,                                                       /* tp_as_number */
    0,                                                       /* tp_as_sequence */
    0,                                                       /* tp_as_mapping */
    0,                                                       /* tp_hash */
    0,                                                       /* tp_call */
    0,                                                       /* tp_str */
    0,                                                       /* tp_getattro */
    0,                                                       /* tp_setattro */
    0,                                                       /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,                /* tp_flags */
    cursor_doc,                                              /* tp_doc */
    0,                                                       /* tp_traverse */
    0,                                                       /* tp_clear */
    0,                                                       /* tp_richcompare */
    0,                 // offsetof(duckdb_Cursor, in_weakreflist),              /* tp_weaklistoffset */
    PyObject_SelfIter, /* tp_iter */
    (iternextfunc)duckdb_cursor_iternext, /* tp_iternext */
    cursor_methods,                       /* tp_methods */
    cursor_members,                       /* tp_members */
    0,                                    /* tp_getset */
    0,                                    /* tp_base */
    0,                                    /* tp_dict */
    0,                                    /* tp_descr_get */
    0,                                    /* tp_descr_set */
    0,                                    /* tp_dictoffset */
    (initproc)duckdb_cursor_init,         /* tp_init */
    0,                                    /* tp_alloc */
    0,                                    /* tp_new */
    0                                     /* tp_free */
};

#if PY_MAJOR_VERSION >= 3
static void *duckdb_pandas_init() {
	if (PyArray_API == NULL) {
		import_array();
	}
	return NULL;
}
#else
static void duckdb_pandas_init() {
	if (PyArray_API == NULL) {
		import_array();
	}
}
#endif

extern int duckdb_cursor_setup_types(void) {
	duckdb_pandas_init();

	PyObject *pandas = PyImport_Import(PyUnicode_FromString("pandas"));
	if (!pandas) {
		return -1;
	}
	PyObject *dataframe = PyObject_GetAttrString(pandas, "DataFrame");
	if (!dataframe) {
		return -1;
	}
	fromdict_ref = PyObject_GetAttrString(dataframe, "from_dict");
	if (!fromdict_ref) {
		return -1;
	}

	mafunc_ref = PyObject_GetAttrString(PyImport_Import(PyUnicode_FromString("numpy.ma")), "masked_array");
	if (!mafunc_ref) {
		return -1;
	}

	duckdb_CursorType.tp_new = PyType_GenericNew;
	return PyType_Ready(&duckdb_CursorType);
}

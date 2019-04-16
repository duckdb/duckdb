#include "cursor.h"

#include "module.h"
#include "pandas.h"

PyObject *duckdb_cursor_iternext(duckdb_Cursor *self);

static const char errmsg_fetch_across_rollback[] =
    "Cursor needed to be reset because of commit/rollback and can no longer be fetched from.";

static int duckdb_cursor_init(duckdb_Cursor *self, PyObject *args, PyObject *kwargs) {
	duckdb_Connection *connection;

	if (!PyArg_ParseTuple(args, "O!", &duckdb_ConnectionType, &connection)) {
		return -1;
	}

	Py_INCREF(connection);
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

PyObject *duckdb_cursor_execute(duckdb_Cursor *self, PyObject *args) {
	PyObject *operation;

	if (!check_cursor(self)) {
	//FIXME	goto error;
	}

	duckdb_cursor_close(self, NULL);
	self->reset = 0;

	if (!PyArg_ParseTuple(args, "O&|", PyUnicode_FSConverter, &operation)) {
		return NULL;
	}

	self->rowcount = 0L;

	char *sql = PyBytes_AsString(operation);
	if (!sql) {
		goto error;
	}

	self->result = self->connection->conn->Query(sql);
	if (!self->result->success) {
		PyErr_SetString(duckdb_DatabaseError, self->result->error.c_str());
		goto error;
	}

	self->closed = 0;
	self->rowcount = self->result->collection.count;

error:
	if (PyErr_Occurred()) {
		return NULL;
	} else {
		Py_INCREF(self);
		return (PyObject *)self;
	}
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

PyObject *duckdb_cursor_fetchnumpy(duckdb_Cursor *self) {
	// TODO make sure there is an active result set

	PyObject *col_dict = PyDict_New();
	for (size_t col_idx = 0; col_idx < self->result->collection.column_count(); col_idx++) {
		PyObject *arr = (PyObject *)PyMaskedArray_FromCol(self->result.get(), col_idx);
		if (!arr) {
			PyErr_SetString(duckdb_DatabaseError, "conversion error");
			return NULL;
		}
		PyDict_SetItem(col_dict, PyUnicode_FromString(self->result->names[col_idx].c_str()), arr);
	}


	Py_INCREF(col_dict);
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
		case duckdb::TypeId::BOOLEAN:
		case duckdb::TypeId::TINYINT:
			val = Py_BuildValue("b", dval.value_.tinyint);
			break;
		case duckdb::TypeId::SMALLINT:
			val = Py_BuildValue("h", dval.value_.smallint);
			break;
		case duckdb::TypeId::INTEGER:
			val = Py_BuildValue("i", dval.value_.integer);
			break;
		case duckdb::TypeId::BIGINT:
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

static PyMethodDef cursor_methods[] = {
    {"execute", (PyCFunction)duckdb_cursor_execute, METH_VARARGS, PyDoc_STR("Executes a SQL statement.")},
    {"fetchone", (PyCFunction)duckdb_cursor_fetchone, METH_NOARGS, PyDoc_STR("Fetches one row from the resultset.")},
    {"fetchall", (PyCFunction)duckdb_cursor_fetchall, METH_NOARGS, PyDoc_STR("Fetches all rows from the resultset.")},
    {"fetchnumpy", (PyCFunction)duckdb_cursor_fetchnumpy, METH_NOARGS,
     PyDoc_STR("Fetches all rows from the  resultset as a dict of numpy arrays.")},
    {"fetchdf", (PyCFunction)duckdb_cursor_fetchdf, METH_NOARGS,
     PyDoc_STR("Fetches all rows from the result set as a pandas DataFrame.")},
    {"close", (PyCFunction)duckdb_cursor_close, METH_NOARGS, PyDoc_STR("Closes the cursor.")},
    {NULL, NULL}};

//      {"fetchall", (PyCFunction)duckdb_cursor_fetchall, METH_NOARGS, PyDoc_STR("Fetches all rows from the
//      resultset.")},

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

static void *duckdb_pandas_init() {
	if (PyArray_API == NULL) {
		import_array();
	}
	return NULL;
}

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

	duckdb_CursorType.tp_new = PyType_GenericNew;
	return PyType_Ready(&duckdb_CursorType);
}

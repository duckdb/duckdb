#include "cursor.h"

#include "module.h"

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
		goto error;
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

	duckdb_query(self->connection->conn, sql, &self->result);
	if (self->result.error_message) {
		PyErr_SetString(duckdb_DatabaseError, self->result.error_message);
		goto error;
	}
	self->closed = 0;
	self->rowcount = self->result.row_count;

error:
	if (PyErr_Occurred()) {
		return NULL;
	} else {
		Py_INCREF(self);
		return (PyObject *)self;
	}
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

	PyObject *row = PyList_New(self->result.column_count);

	// TODO
	//	DUCKDB_TYPE_TIMESTAMP,
	//	DUCKDB_TYPE_DATE,
	//	DUCKDB_TYPE_VARCHAR,

	for (size_t col_idx = 0; col_idx < self->result.column_count; col_idx++) {
		PyObject *val = NULL;
		duckdb_column col = self->result.columns[col_idx];
		switch (col.type) {
		case DUCKDB_TYPE_BOOLEAN:
		case DUCKDB_TYPE_TINYINT:
			val = Py_BuildValue("b", ((int8_t *)col.data)[self->offset]);
			break;
		case DUCKDB_TYPE_SMALLINT:
			val = Py_BuildValue("h", ((int16_t *)col.data)[self->offset]);
			break;
		case DUCKDB_TYPE_INTEGER:
			val = Py_BuildValue("i", ((int32_t *)col.data)[self->offset]);
			break;
		case DUCKDB_TYPE_BIGINT:
			val = Py_BuildValue("L", ((int64_t *)col.data)[self->offset]);
			break;
		case DUCKDB_TYPE_DECIMAL:
			val = Py_BuildValue("d", ((double *)col.data)[self->offset]);
			break;
		case DUCKDB_TYPE_VARCHAR:
			val = Py_BuildValue("s", duckdb_get_value_str(col, self->offset));
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

	duckdb_destroy_result(self->result);

	self->closed = 1;
	self->rowcount = 0;
	self->offset = 0;

	Py_RETURN_NONE;
}

static PyMethodDef cursor_methods[] = {
    {"execute", (PyCFunction)duckdb_cursor_execute, METH_VARARGS, PyDoc_STR("Executes a SQL statement.")},
    //  {"fetchone", (PyCFunction)duckdb_cursor_fetchone, METH_NOARGS, PyDoc_STR("Fetches one row from the
    //  resultset.")},
    //  {"fetchall", (PyCFunction)duckdb_cursor_fetchall, METH_NOARGS, PyDoc_STR("Fetches all rows from the
    //  resultset.")},
    {"close", (PyCFunction)duckdb_cursor_close, METH_NOARGS, PyDoc_STR("Closes the cursor.")},
    {NULL, NULL}};

static struct PyMemberDef cursor_members[] = {
    //    {"connection", T_OBJECT, offsetof(pysqlite_Cursor, connection), READONLY},
    //    {"description", T_OBJECT, offsetof(pysqlite_Cursor, description), READONLY},
    //    {"arraysize", T_INT, offsetof(pysqlite_Cursor, arraysize), 0},
    //    {"lastrowid", T_OBJECT, offsetof(pysqlite_Cursor, lastrowid), READONLY},
    {"rowcount", T_LONG, offsetof(duckdb_Cursor, rowcount), READONLY},
    //    {"row_factory", T_OBJECT, offsetof(pysqlite_Cursor, row_factory), 0},
    {NULL}};

static const char cursor_doc[] = PyDoc_STR("SQLite database cursor class.");

PyTypeObject duckdb_CursorType = {
    PyVarObject_HEAD_INIT(NULL, 0) MODULE_NAME ".Cursor", /* tp_name */
    sizeof(duckdb_Cursor),                                /* tp_basicsize */
    0,                                                    /* tp_itemsize */
    (destructor)duckdb_cursor_dealloc,                    /* tp_dealloc */
    0,                                                    /* tp_print */
    0,                                                    /* tp_getattr */
    0,                                                    /* tp_setattr */
    0,                                                    /* tp_reserved */
    0,                                                    /* tp_repr */
    0,                                                    /* tp_as_number */
    0,                                                    /* tp_as_sequence */
    0,                                                    /* tp_as_mapping */
    0,                                                    /* tp_hash */
    0,                                                    /* tp_call */
    0,                                                    /* tp_str */
    0,                                                    /* tp_getattro */
    0,                                                    /* tp_setattro */
    0,                                                    /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,             /* tp_flags */
    cursor_doc,                                           /* tp_doc */
    0,                                                    /* tp_traverse */
    0,                                                    /* tp_clear */
    0,                                                    /* tp_richcompare */
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

extern int duckdb_cursor_setup_types(void) {
	duckdb_CursorType.tp_new = PyType_GenericNew;
	return PyType_Ready(&duckdb_CursorType);
}

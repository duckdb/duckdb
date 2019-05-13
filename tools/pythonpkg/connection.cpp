#include "connection.h"

#include "cursor.h"
#include "duckdb.hpp"
#include "module.h"
#include "pythread.h"

int duckdb_connection_init(duckdb_Connection *self, PyObject *args, PyObject *kwargs) {
	static char *kwlist[] = {"database", NULL};

	char *database;
	PyObject *database_obj;

	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O&|", kwlist,
#if PY_MAJOR_VERSION >= 3
	                                 PyUnicode_FSConverter,
#endif
	                                 &database_obj)) {
		return -1;
	}

	database = PyBytes_AsString(database_obj);
	Py_DECREF(database_obj);

	Py_BEGIN_ALLOW_THREADS;
	try {
		self->db = duckdb::make_unique<duckdb::DuckDB>(database);
		self->conn = duckdb::make_unique<duckdb::Connection>(*self->db.get());
	} catch (...) {
		return -1;
	}
	Py_END_ALLOW_THREADS;

	self->initialized = 1;
	self->DatabaseError = duckdb_DatabaseError;
	return 0;
}

void duckdb_connection_dealloc(duckdb_Connection *self) {
	duckdb_connection_close(self, NULL);
	Py_TYPE(self)->tp_free((PyObject *)self);
}

PyObject *duckdb_connection_cursor(duckdb_Connection *self, PyObject *args, PyObject *kwargs) {
	PyObject *cursor;

	if (!duckdb_check_connection(self)) {
		return NULL;
	}

	cursor = PyObject_CallFunctionObjArgs((PyObject *)&duckdb_CursorType, (PyObject *)self, NULL);
	if (cursor == NULL)
		return NULL;
	if (!PyObject_TypeCheck(cursor, &duckdb_CursorType)) {
		PyErr_Format(PyExc_TypeError, "factory must return a cursor, not %.100s", Py_TYPE(cursor)->tp_name);
		Py_DECREF(cursor);
		return NULL;
	}

	return cursor;
}

PyObject *duckdb_connection_close(duckdb_Connection *self, PyObject *args) {
	if (self->db) {
		Py_BEGIN_ALLOW_THREADS;
		self->conn = nullptr;
		self->db = nullptr;
		Py_END_ALLOW_THREADS;
	}
	Py_RETURN_NONE;
}

/*
 * Checks if a connection object is usable (i. e. not closed).
 *
 * 0 => error; 1 => ok
 */
int duckdb_check_connection(duckdb_Connection *con) {
	if (!con->initialized) {
		PyErr_SetString(duckdb_DatabaseError, "Base Connection.__init__ not called.");
		return 0;
	}

	if (!con->db) {
		PyErr_SetString(duckdb_DatabaseError, "Cannot operate on a closed database.");
		return 0;
	} else {
		return 1;
	}
}

static const char connection_doc[] = PyDoc_STR("DuckDB database connection object.");

static PyGetSetDef connection_getset[] = {
    //    {"in_transaction",  (getter)duckdb_connection_get_in_transaction, (setter)0},
    {NULL}};

static PyMethodDef connection_methods[] = {
    {"cursor", (PyCFunction)(void (*)(void))duckdb_connection_cursor, METH_VARARGS | METH_KEYWORDS,
     PyDoc_STR("Return a cursor for the connection.")},
    {"close", (PyCFunction)duckdb_connection_close, METH_NOARGS, PyDoc_STR("Closes the connection.")},
    //    {"commit", (PyCFunction)duckdb_connection_commit, METH_NOARGS, PyDoc_STR("Commit the current transaction.")},
    //    {"rollback", (PyCFunction)duckdb_connection_rollback, METH_NOARGS, PyDoc_STR("Roll back the current
    //    transaction.")},
    {NULL, NULL}};

static struct PyMemberDef connection_members[] = {
    {"Error", T_OBJECT, offsetof(duckdb_Connection, DatabaseError), READONLY}, {NULL}};

PyTypeObject duckdb_ConnectionType = {
    PyVarObject_HEAD_INIT(NULL, 0) MODULE_NAME ".Connection", /* tp_name */
    sizeof(duckdb_Connection),                                /* tp_basicsize */
    0,                                                        /* tp_itemsize */
    (destructor)duckdb_connection_dealloc,                    /* tp_dealloc */
    0,                                                        /* tp_print */
    0,                                                        /* tp_getattr */
    0,                                                        /* tp_setattr */
    0,                                                        /* tp_reserved */
    0,                                                        /* tp_repr */
    0,                                                        /* tp_as_number */
    0,                                                        /* tp_as_sequence */
    0,                                                        /* tp_as_mapping */
    0,                                                        /* tp_hash */
    0,                                        //(ternaryfunc)duckdb_connection_call,                      /* tp_call */
    0,                                        /* tp_str */
    0,                                        /* tp_getattro */
    0,                                        /* tp_setattro */
    0,                                        /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, /* tp_flags */
    connection_doc,                           /* tp_doc */
    0,                                        /* tp_traverse */
    0,                                        /* tp_clear */
    0,                                        /* tp_richcompare */
    0,                                        /* tp_weaklistoffset */
    0,                                        /* tp_iter */
    0,                                        /* tp_iternext */
    connection_methods,                       /* tp_methods */
    connection_members,                       /* tp_members */
    connection_getset,                        /* tp_getset */
    0,                                        /* tp_base */
    0,                                        /* tp_dict */
    0,                                        /* tp_descr_get */
    0,                                        /* tp_descr_set */
    0,                                        /* tp_dictoffset */
    (initproc)duckdb_connection_init,         /* tp_init */
    0,                                        /* tp_alloc */
    0,                                        /* tp_new */
    0                                         /* tp_free */
};

extern int duckdb_connection_setup_types(void) {
	duckdb_ConnectionType.tp_new = PyType_GenericNew;
	return PyType_Ready(&duckdb_ConnectionType);
}

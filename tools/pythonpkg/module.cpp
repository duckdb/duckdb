#include "module.h"

#include "connection.h"
#include "cursor.h"

/* static objects at module-level */

PyObject *duckdb_DatabaseError = NULL;

static PyObject *module_connect(PyObject *self, PyObject *args, PyObject *kwargs) {
	return PyObject_Call((PyObject *)&duckdb_ConnectionType, args, kwargs);
}

PyDoc_STRVAR(module_connect_doc, "connect(database)\n\
\n\
Opens a connection to the DuckDB database file *database*. You can use\n\
\":memory:\" to open a database connection to a database that resides in\n\
RAM instead of on disk.");

static PyMethodDef module_methods[] = {
    {"connect", (PyCFunction)(void (*)(void))module_connect, METH_VARARGS | METH_KEYWORDS, module_connect_doc},
    {NULL, NULL}};

#if PY_MAJOR_VERSION >= 3

static struct PyModuleDef _duckdbmodule = {
    PyModuleDef_HEAD_INIT, "duckdb", NULL, -1, module_methods, NULL, NULL, NULL, NULL};

PyMODINIT_FUNC PyInit_duckdb(void)

#else // Python 2
extern "C" {
void initduckdb(void)

#endif
{
	PyObject *module, *dict;

#if PY_MAJOR_VERSION >= 3
	module = PyModule_Create(&_duckdbmodule);
#else
	module = Py_InitModule("duckdb", module_methods);
#endif

	if (!module || (duckdb_cursor_setup_types() < 0) || (duckdb_connection_setup_types() < 0)) {
		goto error;
	}

	Py_INCREF(&duckdb_ConnectionType);
	PyModule_AddObject(module, "Connection", (PyObject *)&duckdb_ConnectionType);
	Py_INCREF(&duckdb_CursorType);
	PyModule_AddObject(module, "Cursor", (PyObject *)&duckdb_CursorType);

	if (!(dict = PyModule_GetDict(module))) {
		goto error;
	}

	/*** Create DB-API Exception hierarchy */
	if (!(duckdb_DatabaseError = PyErr_NewException(MODULE_NAME ".DatabaseError", PyExc_Exception, NULL))) {
		goto error;
	}
	PyDict_SetItemString(dict, "DatabaseError", duckdb_DatabaseError);

error:
	if (PyErr_Occurred()) {
		PyErr_SetString(PyExc_ImportError, MODULE_NAME ": init failed");
		Py_DECREF(module);
		module = NULL;
	}
#if PY_MAJOR_VERSION >= 3
	return module;
#else
} // extern "C"
#endif
}

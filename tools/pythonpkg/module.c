#include "module.h"

#include "connection.h"
#include "statement.h"
#include "cursor.h"
#include "row.h"


/* static objects at module-level */

PyObject *duckdb_Error = NULL;
PyObject *duckdb_Warning = NULL;
PyObject *duckdb_InterfaceError = NULL;
PyObject *duckdb_DatabaseError = NULL;
PyObject *duckdb_InternalError = NULL;
PyObject *duckdb_OperationalError = NULL;
PyObject *duckdb_ProgrammingError = NULL;
PyObject *duckdb_IntegrityError = NULL;
PyObject *duckdb_DataError = NULL;
PyObject *duckdb_NotSupportedError = NULL;

PyObject* _duckdb_converters = NULL;
int _duckdb_enable_callback_tracebacks = 0;
int duckdb_BaseTypeAdapted = 0;

static PyObject* module_connect(PyObject* self, PyObject* args, PyObject*
        kwargs)
{
    return PyObject_Call((PyObject *)&duckdb_ConnectionType, args, kwargs);
}

PyDoc_STRVAR(module_connect_doc,
"connect(database[, timeout, detect_types, isolation_level,\n\
        check_same_thread, factory, cached_statements, uri])\n\
\n\
Opens a connection to the DuckDB database file *database*. You can use\n\
\":memory:\" to open a database connection to a database that resides in\n\
RAM instead of on disk.");


static PyMethodDef module_methods[] = {
    {"connect",  (PyCFunction)(void(*)(void))module_connect,
     METH_VARARGS | METH_KEYWORDS, module_connect_doc},
    {NULL, NULL}
};



static struct PyModuleDef _duckdbmodule = {
        PyModuleDef_HEAD_INIT,
        "duckdb",
        NULL,
        -1,
        module_methods,
        NULL,
        NULL,
        NULL,
        NULL
};

PyMODINIT_FUNC PyInit_duckdb(void)
{
    PyObject *module, *dict;

    module = PyModule_Create(&_duckdbmodule);

    if (!module ||
        (duckdb_row_setup_types() < 0) ||
        (duckdb_cursor_setup_types() < 0) ||
        (duckdb_connection_setup_types() < 0) ||
        (duckdb_statement_setup_types() < 0) // ||
       ) {
        Py_XDECREF(module);
        return NULL;
    }

    Py_INCREF(&duckdb_ConnectionType);
    PyModule_AddObject(module, "Connection", (PyObject*) &duckdb_ConnectionType);
    Py_INCREF(&duckdb_CursorType);
    PyModule_AddObject(module, "Cursor", (PyObject*) &duckdb_CursorType);
   // Py_INCREF(&duckdb_CacheType);
    PyModule_AddObject(module, "Statement", (PyObject*)&duckdb_StatementType);
    Py_INCREF(&duckdb_StatementType);
    //PyModule_AddObject(module, "Cache", (PyObject*) &duckdb_CacheType);
//    Py_INCREF(&duckdb_PrepareProtocolType);
//    PyModule_AddObject(module, "PrepareProtocol", (PyObject*) &duckdb_PrepareProtocolType);
    Py_INCREF(&duckdb_RowType);
    PyModule_AddObject(module, "Row", (PyObject*) &duckdb_RowType);

    if (!(dict = PyModule_GetDict(module))) {
        goto error;
    }

    /*** Create DB-API Exception hierarchy */

    if (!(duckdb_Error = PyErr_NewException(MODULE_NAME ".Error", PyExc_Exception, NULL))) {
        goto error;
    }
    PyDict_SetItemString(dict, "Error", duckdb_Error);

    if (!(duckdb_Warning = PyErr_NewException(MODULE_NAME ".Warning", PyExc_Exception, NULL))) {
        goto error;
    }
    PyDict_SetItemString(dict, "Warning", duckdb_Warning);

    /* Error subclasses */

    if (!(duckdb_InterfaceError = PyErr_NewException(MODULE_NAME ".InterfaceError", duckdb_Error, NULL))) {
        goto error;
    }
    PyDict_SetItemString(dict, "InterfaceError", duckdb_InterfaceError);

    if (!(duckdb_DatabaseError = PyErr_NewException(MODULE_NAME ".DatabaseError", duckdb_Error, NULL))) {
        goto error;
    }
    PyDict_SetItemString(dict, "DatabaseError", duckdb_DatabaseError);

    /* duckdb_DatabaseError subclasses */

    if (!(duckdb_InternalError = PyErr_NewException(MODULE_NAME ".InternalError", duckdb_DatabaseError, NULL))) {
        goto error;
    }
    PyDict_SetItemString(dict, "InternalError", duckdb_InternalError);

    if (!(duckdb_OperationalError = PyErr_NewException(MODULE_NAME ".OperationalError", duckdb_DatabaseError, NULL))) {
        goto error;
    }
    PyDict_SetItemString(dict, "OperationalError", duckdb_OperationalError);

    if (!(duckdb_ProgrammingError = PyErr_NewException(MODULE_NAME ".ProgrammingError", duckdb_DatabaseError, NULL))) {
        goto error;
    }
    PyDict_SetItemString(dict, "ProgrammingError", duckdb_ProgrammingError);

    if (!(duckdb_IntegrityError = PyErr_NewException(MODULE_NAME ".IntegrityError", duckdb_DatabaseError,NULL))) {
        goto error;
    }
    PyDict_SetItemString(dict, "IntegrityError", duckdb_IntegrityError);

    if (!(duckdb_DataError = PyErr_NewException(MODULE_NAME ".DataError", duckdb_DatabaseError, NULL))) {
        goto error;
    }
    PyDict_SetItemString(dict, "DataError", duckdb_DataError);

    if (!(duckdb_NotSupportedError = PyErr_NewException(MODULE_NAME ".NotSupportedError", duckdb_DatabaseError, NULL))) {
        goto error;
    }
    PyDict_SetItemString(dict, "NotSupportedError", duckdb_NotSupportedError);


error:
    if (PyErr_Occurred())
    {
        PyErr_SetString(PyExc_ImportError, MODULE_NAME ": init failed");
        Py_DECREF(module);
        module = NULL;
    }
    return module;
}

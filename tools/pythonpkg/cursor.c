#include "cursor.h"
#include "module.h"

PyObject* duckdb_cursor_iternext(duckdb_Cursor* self);

static const char errmsg_fetch_across_rollback[] = "Cursor needed to be reset because of commit/rollback and can no longer be fetched from.";

static int duckdb_cursor_init(duckdb_Cursor* self, PyObject* args, PyObject* kwargs)
{
    duckdb_Connection* connection;

    if (!PyArg_ParseTuple(args, "O!", &duckdb_ConnectionType, &connection))
    {
        return -1;
    }

    Py_INCREF(connection);
    Py_XSETREF(self->connection, connection);
    Py_CLEAR(self->statement);
    Py_CLEAR(self->next_row);
    Py_CLEAR(self->row_cast_map);

    Py_INCREF(Py_None);
    Py_XSETREF(self->description, Py_None);

    Py_INCREF(Py_None);
    Py_XSETREF(self->lastrowid, Py_None);

    self->arraysize = 1;
    self->closed = 0;
    self->reset = 0;

    self->rowcount = -1L;

    Py_INCREF(Py_None);
    Py_XSETREF(self->row_factory, Py_None);

    if (!duckdb_check_thread(self->connection)) {
        return -1;
    }

    if (!duckdb_connection_register_cursor(connection, (PyObject*)self)) {
        return -1;
    }

    self->initialized = 1;

    return 0;
}

static void duckdb_cursor_dealloc(duckdb_Cursor* self)
{
    /* Reset the statement if the user has not closed the cursor */
    if (self->statement) {
        duckdb_statement_reset(self->statement);
        Py_DECREF(self->statement);
    }

    Py_XDECREF(self->connection);
    Py_XDECREF(self->row_cast_map);
    Py_XDECREF(self->description);
    Py_XDECREF(self->lastrowid);
    Py_XDECREF(self->row_factory);
    Py_XDECREF(self->next_row);

    if (self->in_weakreflist != NULL) {
        PyObject_ClearWeakRefs((PyObject*)self);
    }

    Py_TYPE(self)->tp_free((PyObject*)self);
}


/*
 * Checks if a cursor object is usable.
 *
 * 0 => error; 1 => ok
 */
static int check_cursor(duckdb_Cursor* cur)
{
    if (!cur->initialized) {
        PyErr_SetString(duckdb_ProgrammingError, "Base Cursor.__init__ not called.");
        return 0;
    }

    if (cur->closed) {
        PyErr_SetString(duckdb_ProgrammingError, "Cannot operate on a closed cursor.");
        return 0;
    }

    if (cur->locked) {
        PyErr_SetString(duckdb_ProgrammingError, "Recursive use of cursors not allowed.");
        return 0;
    }

    return duckdb_check_thread(cur->connection) && duckdb_check_connection(cur->connection);
}


//
//static PyObject *
//_pysqlite_query_execute(pysqlite_Cursor* self, int multiple, PyObject* args)
//{
//    PyObject* operation;
//    PyObject* parameters_list = NULL;
//    PyObject* parameters_iter = NULL;
//    PyObject* parameters = NULL;
//    int i;
//    int rc;
//    PyObject* func_args;
//    PyObject* result;
//    int numcols;
//    PyObject* descriptor;
//    PyObject* second_argument = NULL;
//    sqlite_int64 lastrowid;
//
//    if (!check_cursor(self)) {
//        goto error;
//    }
//
//    self->locked = 1;
//    self->reset = 0;
//
//    Py_CLEAR(self->next_row);
//
//    if (multiple) {
//        /* executemany() */
//        if (!PyArg_ParseTuple(args, "OO", &operation, &second_argument)) {
//            goto error;
//        }
//
//        if (!PyUnicode_Check(operation)) {
//            PyErr_SetString(PyExc_ValueError, "operation parameter must be str");
//            goto error;
//        }
//
//        if (PyIter_Check(second_argument)) {
//            /* iterator */
//            Py_INCREF(second_argument);
//            parameters_iter = second_argument;
//        } else {
//            /* sequence */
//            parameters_iter = PyObject_GetIter(second_argument);
//            if (!parameters_iter) {
//                goto error;
//            }
//        }
//    } else {
//        /* execute() */
//        if (!PyArg_ParseTuple(args, "O|O", &operation, &second_argument)) {
//            goto error;
//        }
//
//        if (!PyUnicode_Check(operation)) {
//            PyErr_SetString(PyExc_ValueError, "operation parameter must be str");
//            goto error;
//        }
//
//        parameters_list = PyList_New(0);
//        if (!parameters_list) {
//            goto error;
//        }
//
//        if (second_argument == NULL) {
//            second_argument = PyTuple_New(0);
//            if (!second_argument) {
//                goto error;
//            }
//        } else {
//            Py_INCREF(second_argument);
//        }
//        if (PyList_Append(parameters_list, second_argument) != 0) {
//            Py_DECREF(second_argument);
//            goto error;
//        }
//        Py_DECREF(second_argument);
//
//        parameters_iter = PyObject_GetIter(parameters_list);
//        if (!parameters_iter) {
//            goto error;
//        }
//    }
//
//    if (self->statement != NULL) {
//        /* There is an active statement */
//        pysqlite_statement_reset(self->statement);
//    }
//
//    /* reset description and rowcount */
//    Py_INCREF(Py_None);
//    Py_SETREF(self->description, Py_None);
//    self->rowcount = 0L;
//
//    func_args = PyTuple_New(1);
//    if (!func_args) {
//        goto error;
//    }
//    Py_INCREF(operation);
//    if (PyTuple_SetItem(func_args, 0, operation) != 0) {
//        goto error;
//    }
//
//    if (self->statement) {
//        (void)pysqlite_statement_reset(self->statement);
//    }
//
//    Py_XSETREF(self->statement,
//              (pysqlite_Statement *)pysqlite_cache_get(self->connection->statement_cache, func_args));
//    Py_DECREF(func_args);
//
//    if (!self->statement) {
//        goto error;
//    }
//
//    if (self->statement->in_use) {
//        Py_SETREF(self->statement,
//                  PyObject_New(pysqlite_Statement, &pysqlite_StatementType));
//        if (!self->statement) {
//            goto error;
//        }
//        rc = pysqlite_statement_create(self->statement, self->connection, operation);
//        if (rc != SQLITE_OK) {
//            Py_CLEAR(self->statement);
//            goto error;
//        }
//    }
//
//    pysqlite_statement_reset(self->statement);
//    pysqlite_statement_mark_dirty(self->statement);
//
//    /* We start a transaction implicitly before a DML statement.
//       SELECT is the only exception. See #9924. */
//    if (self->connection->begin_statement && self->statement->is_dml) {
//        if (sqlite3_get_autocommit(self->connection->db)) {
//            result = _pysqlite_connection_begin(self->connection);
//            if (!result) {
//                goto error;
//            }
//            Py_DECREF(result);
//        }
//    }
//
//    while (1) {
//        parameters = PyIter_Next(parameters_iter);
//        if (!parameters) {
//            break;
//        }
//
//        pysqlite_statement_mark_dirty(self->statement);
//
//        pysqlite_statement_bind_parameters(self->statement, parameters);
//        if (PyErr_Occurred()) {
//            goto error;
//        }
//
//        rc = pysqlite_step(self->statement->st, self->connection);
//        if (rc != SQLITE_DONE && rc != SQLITE_ROW) {
//            if (PyErr_Occurred()) {
//                /* there was an error that occurred in a user-defined callback */
//                if (_pysqlite_enable_callback_tracebacks) {
//                    PyErr_Print();
//                } else {
//                    PyErr_Clear();
//                }
//            }
//            (void)pysqlite_statement_reset(self->statement);
//            _pysqlite_seterror(self->connection->db, NULL);
//            goto error;
//        }
//
//        if (pysqlite_build_row_cast_map(self) != 0) {
//            _PyErr_FormatFromCause(pysqlite_OperationalError, "Error while building row_cast_map");
//            goto error;
//        }
//
//        assert(rc == SQLITE_ROW || rc == SQLITE_DONE);
//        Py_BEGIN_ALLOW_THREADS
//        numcols = sqlite3_column_count(self->statement->st);
//        Py_END_ALLOW_THREADS
//        if (self->description == Py_None && numcols > 0) {
//            Py_SETREF(self->description, PyTuple_New(numcols));
//            if (!self->description) {
//                goto error;
//            }
//            for (i = 0; i < numcols; i++) {
//                descriptor = PyTuple_New(7);
//                if (!descriptor) {
//                    goto error;
//                }
//                PyTuple_SetItem(descriptor, 0, _pysqlite_build_column_name(sqlite3_column_name(self->statement->st, i)));
//                Py_INCREF(Py_None); PyTuple_SetItem(descriptor, 1, Py_None);
//                Py_INCREF(Py_None); PyTuple_SetItem(descriptor, 2, Py_None);
//                Py_INCREF(Py_None); PyTuple_SetItem(descriptor, 3, Py_None);
//                Py_INCREF(Py_None); PyTuple_SetItem(descriptor, 4, Py_None);
//                Py_INCREF(Py_None); PyTuple_SetItem(descriptor, 5, Py_None);
//                Py_INCREF(Py_None); PyTuple_SetItem(descriptor, 6, Py_None);
//                PyTuple_SetItem(self->description, i, descriptor);
//            }
//        }
//
//        if (self->statement->is_dml) {
//            self->rowcount += (long)sqlite3_changes(self->connection->db);
//        } else {
//            self->rowcount= -1L;
//        }
//
//        if (!multiple) {
//            Py_DECREF(self->lastrowid);
//            Py_BEGIN_ALLOW_THREADS
//            lastrowid = sqlite3_last_insert_rowid(self->connection->db);
//            Py_END_ALLOW_THREADS
//            self->lastrowid = _pysqlite_long_from_int64(lastrowid);
//        }
//
//        if (rc == SQLITE_ROW) {
//            if (multiple) {
//                PyErr_SetString(pysqlite_ProgrammingError, "executemany() can only execute DML statements.");
//                goto error;
//            }
//
//            self->next_row = _pysqlite_fetch_one_row(self);
//            if (self->next_row == NULL)
//                goto error;
//        } else if (rc == SQLITE_DONE && !multiple) {
//            pysqlite_statement_reset(self->statement);
//            Py_CLEAR(self->statement);
//        }
//
//        if (multiple) {
//            pysqlite_statement_reset(self->statement);
//        }
//        Py_XDECREF(parameters);
//    }
//
//error:
//    Py_XDECREF(parameters);
//    Py_XDECREF(parameters_iter);
//    Py_XDECREF(parameters_list);
//
//    self->locked = 0;
//
//    if (PyErr_Occurred()) {
//        self->rowcount = -1L;
//        return NULL;
//    } else {
//        Py_INCREF(self);
//        return (PyObject*)self;
//    }
//}

PyObject* duckdb_cursor_execute(duckdb_Cursor* self, PyObject* args)
{
    //return _pysqlite_query_execute(self, 0, args);
	return NULL;
}


PyObject* duckdb_cursor_iternext(duckdb_Cursor *self)
{
    PyObject* next_row_tuple;
    PyObject* next_row;
    int rc;

    if (!check_cursor(self)) {
        return NULL;
    }

    if (self->reset) {
        PyErr_SetString(duckdb_InterfaceError, errmsg_fetch_across_rollback);
        return NULL;
    }

    if (!self->next_row) {
         if (self->statement) {
            (void)duckdb_statement_reset(self->statement);
            Py_CLEAR(self->statement);
        }
        return NULL;
    }

    next_row_tuple = self->next_row;
    assert(next_row_tuple != NULL);
    self->next_row = NULL;

    if (self->row_factory != Py_None) {
        next_row = PyObject_CallFunction(self->row_factory, "OO", self, next_row_tuple);
        if (next_row == NULL) {
            self->next_row = next_row_tuple;
            return NULL;
        }
        Py_DECREF(next_row_tuple);
    } else {
        next_row = next_row_tuple;
    }

    if (self->statement) {
        // TODO
    	//rc = pysqlite_step(self->statement->st, self->connection);
        	rc = 0;
//    	if (PyErr_Occurred()) {
//            (void)duckdb_statement_reset(self->statement);
//            Py_DECREF(next_row);
//            return NULL;
//        }
//        if (rc != SQLITE_DONE && rc != SQLITE_ROW) {
//            (void)duckdb_statement_reset(self->statement);
//            Py_DECREF(next_row);
//           // _pysqlite_seterror(self->connection->db, NULL);
//            return NULL;
//        }
//
//        if (rc == SQLITE_ROW) {
//           // TODO FIXME
//        	// self->next_row = _pysqlite_fetch_one_row(self);
//            if (self->next_row == NULL) {
//                (void)duckdb_statement_reset(self->statement);
//                return NULL;
//            }
//        }
    }

    return next_row;
}

PyObject* duckdb_cursor_fetchone(duckdb_Cursor* self, PyObject* args)
{
    PyObject* row;

    row = duckdb_cursor_iternext(self);
    if (!row && !PyErr_Occurred()) {
        Py_RETURN_NONE;
    }

    return row;
}

PyObject* duckdb_cursor_fetchall(duckdb_Cursor* self, PyObject* args)
{
    PyObject* row;
    PyObject* list;

    list = PyList_New(0);
    if (!list) {
        return NULL;
    }

    /* just make sure we enter the loop */
    row = (PyObject*)Py_None;

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
//
//PyObject* pysqlite_noop(duckdb_Connection* self, PyObject* args)
//{
//    /* don't care, return None */
//    Py_RETURN_NONE;
//}

PyObject* duckdb_cursor_close(duckdb_Cursor* self, PyObject* args)
{
    if (!self->connection) {
        PyErr_SetString(duckdb_ProgrammingError,
                        "Base Cursor.__init__ not called.");
        return NULL;
    }
    if (!duckdb_check_thread(self->connection) || !duckdb_check_connection(self->connection)) {
        return NULL;
    }

    if (self->statement) {
        (void)duckdb_statement_reset(self->statement);
        Py_CLEAR(self->statement);
    }

    self->closed = 1;

    Py_RETURN_NONE;
}

static PyMethodDef cursor_methods[] = {
    {"execute", (PyCFunction)duckdb_cursor_execute, METH_VARARGS,
        PyDoc_STR("Executes a SQL statement.")},
    {"fetchone", (PyCFunction)duckdb_cursor_fetchone, METH_NOARGS,
        PyDoc_STR("Fetches one row from the resultset.")},
    {"fetchall", (PyCFunction)duckdb_cursor_fetchall, METH_NOARGS,
        PyDoc_STR("Fetches all rows from the resultset.")},
    {"close", (PyCFunction)duckdb_cursor_close, METH_NOARGS,
        PyDoc_STR("Closes the cursor.")},
    {NULL, NULL}
};

static struct PyMemberDef cursor_members[] =
{
//    {"connection", T_OBJECT, offsetof(pysqlite_Cursor, connection), READONLY},
//    {"description", T_OBJECT, offsetof(pysqlite_Cursor, description), READONLY},
//    {"arraysize", T_INT, offsetof(pysqlite_Cursor, arraysize), 0},
//    {"lastrowid", T_OBJECT, offsetof(pysqlite_Cursor, lastrowid), READONLY},
//    {"rowcount", T_LONG, offsetof(pysqlite_Cursor, rowcount), READONLY},
//    {"row_factory", T_OBJECT, offsetof(pysqlite_Cursor, row_factory), 0},
    {NULL}
};

static const char cursor_doc[] =
PyDoc_STR("SQLite database cursor class.");

PyTypeObject duckdb_CursorType = {
        PyVarObject_HEAD_INIT(NULL, 0)
        MODULE_NAME ".Cursor",                          /* tp_name */
        sizeof(duckdb_Cursor),                        /* tp_basicsize */
        0,                                              /* tp_itemsize */
        (destructor)duckdb_cursor_dealloc,            /* tp_dealloc */
        0,                                              /* tp_print */
        0,                                              /* tp_getattr */
        0,                                              /* tp_setattr */
        0,                                              /* tp_reserved */
        0,                                              /* tp_repr */
        0,                                              /* tp_as_number */
        0,                                              /* tp_as_sequence */
        0,                                              /* tp_as_mapping */
        0,                                              /* tp_hash */
        0,                                              /* tp_call */
        0,                                              /* tp_str */
        0,                                              /* tp_getattro */
        0,                                              /* tp_setattro */
        0,                                              /* tp_as_buffer */
        Py_TPFLAGS_DEFAULT|Py_TPFLAGS_BASETYPE, /* tp_flags */
        cursor_doc,                                     /* tp_doc */
        0,                                              /* tp_traverse */
        0,                                              /* tp_clear */
        0,                                              /* tp_richcompare */
        offsetof(duckdb_Cursor, in_weakreflist),      /* tp_weaklistoffset */
        PyObject_SelfIter,                              /* tp_iter */
        (iternextfunc)duckdb_cursor_iternext,         /* tp_iternext */
        cursor_methods,                                 /* tp_methods */
        cursor_members,                                 /* tp_members */
        0,                                              /* tp_getset */
        0,                                              /* tp_base */
        0,                                              /* tp_dict */
        0,                                              /* tp_descr_get */
        0,                                              /* tp_descr_set */
        0,                                              /* tp_dictoffset */
        (initproc)duckdb_cursor_init,                 /* tp_init */
        0,                                              /* tp_alloc */
        0,                                              /* tp_new */
        0                                               /* tp_free */
};

extern int duckdb_cursor_setup_types(void)
{
    duckdb_CursorType.tp_new = PyType_GenericNew;
    return PyType_Ready(&duckdb_CursorType);
}

/* connection.c - the connection type
 *
 * Copyright (C) 2004-2010 Gerhard Häring <gh@ghaering.de>
 *
 * This file is part of pysqlite.
 *
 * This software is provided 'as-is', without any express or implied
 * warranty.  In no event will the authors be held liable for any damages
 * arising from the use of this software.
 *
 * Permission is granted to anyone to use this software for any purpose,
 * including commercial applications, and to alter it and redistribute it
 * freely, subject to the following restrictions:
 *
 * 1. The origin of this software must not be misrepresented; you must not
 *    claim that you wrote the original software. If you use this software
 *    in a product, an acknowledgment in the product documentation would be
 *    appreciated but is not required.
 * 2. Altered source versions must be plainly marked as such, and must not be
 *    misrepresented as being the original software.
 * 3. This notice may not be removed or altered from any source distribution.
 */

//#include "cache.h"
#include "module.h"
//#include "structmember.h"
#include "connection.h"
#include "statement.h"
#include "cursor.h"
//#include "prepare_protocol.h"
//#include "util.h"

#include "pythread.h"

#define ACTION_FINALIZE 1
#define ACTION_RESET 2


_Py_IDENTIFIER(cursor);


//static int duckdb_connection_set_isolation_level(duckdb_Connection* self, PyObject* isolation_level, void *Py_UNUSED(ignored));
static void _duckdb_drop_unused_cursor_references(duckdb_Connection* self);


//static void _sqlite3_result_error(sqlite3_context* ctx, const char* errmsg, int len)
//{
//    /* in older SQLite versions, calling sqlite3_result_error in callbacks
//     * triggers a bug in SQLite that leads either to irritating results or
//     * segfaults, depending on the SQLite version */
//#if SQLITE_VERSION_NUMBER >= 3003003
//    sqlite3_result_error(ctx, errmsg, len);
//#else
//    PyErr_SetString(duckdb_OperationalError, errmsg);
//#endif
//}

int duckdb_connection_init(duckdb_Connection* self, PyObject* args, PyObject* kwargs)
{
    static char *kwlist[] = {
        "database",
        NULL
    };

    char* database;
    PyObject* database_obj;

    int check_same_thread = 1;
    double timeout = 5.0;
    int rc;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O", kwlist,
                                     PyUnicode_FSConverter, &database_obj))
    {
        return -1;
    }

    database = PyBytes_AsString(database_obj);

    self->initialized = 1;

    self->begin_statement = NULL;

    Py_CLEAR(self->statements);
    Py_CLEAR(self->cursors);


    Py_BEGIN_ALLOW_THREADS
	rc = duckdb_open ((const char*) database, self->db);
	Py_END_ALLOW_THREADS

    Py_DECREF(database_obj);

    if (rc != DuckDBSuccess) {
        return -1;
    }


    self->created_statements = 0;
    self->created_cursors = 0;

    /* Create lists of weak references to statements/cursors */
    self->statements = PyList_New(0);
    self->cursors = PyList_New(0);
    if (!self->statements || !self->cursors) {
        return -1;
    }

    /* By default, the Cache class INCREFs the factory in its initializer, and
     * decrefs it in its deallocator method. Since this would create a circular
     * reference here, we're breaking it by decrementing self, and telling the
     * cache class to not decref the factory (self) in its deallocator.
     */
    Py_DECREF(self);

    self->timeout = timeout;
    self->check_same_thread = check_same_thread;


    self->Warning               = duckdb_Warning;
    self->Error                 = duckdb_Error;
    self->InterfaceError        = duckdb_InterfaceError;
    self->DatabaseError         = duckdb_DatabaseError;
    self->DataError             = duckdb_DataError;
    self->OperationalError      = duckdb_OperationalError;
    self->IntegrityError        = duckdb_IntegrityError;
    self->InternalError         = duckdb_InternalError;
    self->ProgrammingError      = duckdb_ProgrammingError;
    self->NotSupportedError     = duckdb_NotSupportedError;

    return 0;
}


void duckdb_connection_dealloc(duckdb_Connection* self)
{

    /* Clean up if user has not called .close() explicitly. */
    if (self->db) {
        Py_BEGIN_ALLOW_THREADS
        // TODO duckdb_close
		//SQLITE3_CLOSE(self->db);
        Py_END_ALLOW_THREADS
    }

    Py_XDECREF(self->statements);
    Py_XDECREF(self->cursors);

    Py_TYPE(self)->tp_free((PyObject*)self);
}

/*
 * Registers a cursor with the connection.
 *
 * 0 => error; 1 => ok
 */
int duckdb_connection_register_cursor(duckdb_Connection* connection, PyObject* cursor)
{
    PyObject* weakref;

    weakref = PyWeakref_NewRef((PyObject*)cursor, NULL);
    if (!weakref) {
        goto error;
    }

    if (PyList_Append(connection->cursors, weakref) != 0) {
        Py_CLEAR(weakref);
        goto error;
    }

    Py_DECREF(weakref);

    return 1;
error:
    return 0;
}

PyObject* duckdb_connection_cursor(duckdb_Connection* self, PyObject* args, PyObject* kwargs)
{
    PyObject* cursor;


    if (!duckdb_check_thread(self) || !duckdb_check_connection(self)) {
        return NULL;
    }


    cursor = PyObject_CallFunctionObjArgs((PyObject*)&duckdb_CursorType, (PyObject *)self, NULL);
    if (cursor == NULL)
        return NULL;
    if (!PyObject_TypeCheck(cursor, &duckdb_CursorType)) {
        PyErr_Format(PyExc_TypeError,
                     "factory must return a cursor, not %.100s",
                     Py_TYPE(cursor)->tp_name);
        Py_DECREF(cursor);
        return NULL;
    }

    _duckdb_drop_unused_cursor_references(self);

    if (cursor && self->row_factory != Py_None) {
        Py_INCREF(self->row_factory);
        Py_XSETREF(((duckdb_Cursor *)cursor)->row_factory, self->row_factory);
    }

    return cursor;
}

PyObject* duckdb_connection_close(duckdb_Connection* self, PyObject* args)
{
    int rc;

    if (!duckdb_check_thread(self)) {
        return NULL;
    }

    // FIXME: I think this is not neccessary because the connection cleans itself
    //duckdb_do_all_statements(self, ACTION_FINALIZE, 1);

    if (self->db) {
        Py_BEGIN_ALLOW_THREADS
		rc = duckdb_close(self->db);
		Py_END_ALLOW_THREADS

        if (rc != DuckDBSuccess) {
            return NULL;
        } else {
            self->db = NULL;
        }
    }

    Py_RETURN_NONE;
}

/*
 * Checks if a connection object is usable (i. e. not closed).
 *
 * 0 => error; 1 => ok
 */
int duckdb_check_connection(duckdb_Connection* con)
{
    if (!con->initialized) {
        PyErr_SetString(duckdb_ProgrammingError, "Base Connection.__init__ not called.");
        return 0;
    }

    if (!con->db) {
        PyErr_SetString(duckdb_ProgrammingError, "Cannot operate on a closed database.");
        return 0;
    } else {
        return 1;
    }
}

PyObject* _duckdb_connection_begin(duckdb_Connection* self)
{
//    int rc;
//    const char* tail;
//    duckdb_statement* statement;
//
//    Py_BEGIN_ALLOW_THREADS
//	rc = 0;
// //   rc = duckdb_prepare(self->db, self->begin_statement, -1, &statement, &tail);
//    Py_END_ALLOW_THREADS
//
//    if (rc != DuckDBSuccess) {
//        _duckdb_seterror(self->db, statement);
//        goto error;
//    }
//
//    //rc = duckdb_step(statement, self);
//    if (rc != DuckDBSuccess) {
//        _duckdb_seterror(self->db, statement);
//    }
//
//    Py_BEGIN_ALLOW_THREADS
//  //  rc = sqlite3_finalize(statement);
//    Py_END_ALLOW_THREADS
//
//    if (rc != DuckDBSuccess && !PyErr_Occurred()) {
//        _duckdb_seterror(self->db, NULL);
//    }
//
//error:
//    if (PyErr_Occurred()) {
//        return NULL;
//    } else {
//        Py_RETURN_NONE;
//    }
return NULL;
}

PyObject* duckdb_connection_commit(duckdb_Connection* self, PyObject* args)
{
//    int rc;
//    const char* tail;
//    duckdb_statement* statement;
//
//    if (!duckdb_check_thread(self) || !duckdb_check_connection(self)) {
//        return NULL;
//    }
//
//    if (!sqlite3_get_autocommit(self->db)) {
//
//        Py_BEGIN_ALLOW_THREADS
//        //rc = sqlite3_prepare_v2(self->db, "COMMIT", -1, &statement, &tail);
//        Py_END_ALLOW_THREADS
//        if (rc != DuckDBSuccess) {
//            _duckdb_seterror(self->db, NULL);
//            goto error;
//        }
//
//      //  rc = duckdb_step(statement, self);
//        if (rc != DuckDBSuccess) {
//            _duckdb_seterror(self->db, statement);
//        }
//
//        Py_BEGIN_ALLOW_THREADS
//      //  rc = sqlite3_finalize(statement);
//        Py_END_ALLOW_THREADS
//        if (rc != DuckDBSuccess && !PyErr_Occurred()) {
//            _duckdb_seterror(self->db, NULL);
//        }
//
//    }
//
//error:
//    if (PyErr_Occurred()) {
//        return NULL;
//    } else {
//        Py_RETURN_NONE;
//    }
	return NULL;

}

PyObject* duckdb_connection_rollback(duckdb_Connection* self, PyObject* args)
{
//    int rc;
//    const char* tail;
//    duckdb_statement* statement;
//
//    if (!duckdb_check_thread(self) || !duckdb_check_connection(self)) {
//        return NULL;
//    }
//
//    if (!sqlite3_get_autocommit(self->db)) {
//        duckdb_do_all_statements(self, ACTION_RESET, 1);
//
//        Py_BEGIN_ALLOW_THREADS
//        rc = sqlite3_prepare_v2(self->db, "ROLLBACK", -1, &statement, &tail);
//        Py_END_ALLOW_THREADS
//        if (rc != DuckDBSuccess) {
//            _duckdb_seterror(self->db, NULL);
//            goto error;
//        }
//
//        rc = duckdb_step(statement, self);
//        if (rc != DuckDBSuccess) {
//            _duckdb_seterror(self->db, statement);
//        }
//
//        Py_BEGIN_ALLOW_THREADS
//        rc = sqlite3_finalize(statement);
//        Py_END_ALLOW_THREADS
//        if (rc != DuckDBSuccess && !PyErr_Occurred()) {
//            _duckdb_seterror(self->db, NULL);
//        }
//
//    }
//
//error:
//    if (PyErr_Occurred()) {
//        return NULL;
//    } else {
//        Py_RETURN_NONE;
//    }
	return NULL;
}
static void _duckdb_drop_unused_statement_references(duckdb_Connection* self)
{
    PyObject* new_list;
    PyObject* weakref;
    int i;

    /* we only need to do this once in a while */
    if (self->created_statements++ < 200) {
        return;
    }

    self->created_statements = 0;

    new_list = PyList_New(0);
    if (!new_list) {
        return;
    }

    for (i = 0; i < PyList_Size(self->statements); i++) {
        weakref = PyList_GetItem(self->statements, i);
        if (PyWeakref_GetObject(weakref) != Py_None) {
            if (PyList_Append(new_list, weakref) != 0) {
                Py_DECREF(new_list);
                return;
            }
        }
    }

    Py_SETREF(self->statements, new_list);
}

static void _duckdb_drop_unused_cursor_references(duckdb_Connection* self)
{
    PyObject* new_list;
    PyObject* weakref;
    int i;

    /* we only need to do this once in a while */
    if (self->created_cursors++ < 200) {
        return;
    }

    self->created_cursors = 0;

    new_list = PyList_New(0);
    if (!new_list) {
        return;
    }

    for (i = 0; i < PyList_Size(self->cursors); i++) {
        weakref = PyList_GetItem(self->cursors, i);
        if (PyWeakref_GetObject(weakref) != Py_None) {
            if (PyList_Append(new_list, weakref) != 0) {
                Py_DECREF(new_list);
                return;
            }
        }
    }

    Py_SETREF(self->cursors, new_list);
}

int duckdb_check_thread(duckdb_Connection* self)
{
    if (self->check_same_thread) {
        if (PyThread_get_thread_ident() != self->thread_ident) {
            PyErr_Format(duckdb_ProgrammingError,
                        "SQLite objects created in a thread can only be used in that same thread. "
                        "The object was created in thread id %lu and this is thread id %lu.",
                        self->thread_ident, PyThread_get_thread_ident());
            return 0;
        }

    }
    return 1;
}
//
//static PyObject* duckdb_connection_get_isolation_level(duckdb_Connection* self, void* unused)
//{
//    Py_INCREF(self->isolation_level);
//    return self->isolation_level;
//}
//
//static PyObject* duckdb_connection_get_total_changes(duckdb_Connection* self, void* unused)
//{
//    if (!duckdb_check_connection(self)) {
//        return NULL;
//    } else {
//        return Py_BuildValue("i", sqlite3_total_changes(self->db));
//    }
//}
//
//static PyObject* duckdb_connection_get_in_transaction(duckdb_Connection* self, void* unused)
//{
//    if (!duckdb_check_connection(self)) {
//        return NULL;
//    }
//    if (!sqlite3_get_autocommit(self->db)) {
//        Py_RETURN_TRUE;
//    }
//    Py_RETURN_FALSE;
//}
//
//static int
//duckdb_connection_set_isolation_level(duckdb_Connection* self, PyObject* isolation_level, void *Py_UNUSED(ignored))
//{
//    if (isolation_level == NULL) {
//        PyErr_SetString(PyExc_AttributeError, "cannot delete attribute");
//        return -1;
//    }
//    if (isolation_level == Py_None) {
//        PyObject *res = duckdb_connection_commit(self, NULL);
//        if (!res) {
//            return -1;
//        }
//        Py_DECREF(res);
//
//        self->begin_statement = NULL;
//    } else {
//        const char * const *candidate;
//        PyObject *uppercase_level;
//        _Py_IDENTIFIER(upper);
//
//        if (!PyUnicode_Check(isolation_level)) {
//            PyErr_Format(PyExc_TypeError,
//                         "isolation_level must be a string or None, not %.100s",
//                         Py_TYPE(isolation_level)->tp_name);
//            return -1;
//        }
//
//        uppercase_level = _PyObject_CallMethodIdObjArgs(
//                        (PyObject *)&PyUnicode_Type, &PyId_upper,
//                        isolation_level, NULL);
//        if (!uppercase_level) {
//            return -1;
//        }
//        for (candidate = begin_statements; *candidate; candidate++) {
//            if (_PyUnicode_EqualToASCIIString(uppercase_level, *candidate + 6))
//                break;
//        }
//        Py_DECREF(uppercase_level);
//        if (!*candidate) {
//            PyErr_SetString(PyExc_ValueError,
//                            "invalid value for isolation_level");
//            return -1;
//        }
//        self->begin_statement = *candidate;
//    }
//
//    Py_INCREF(isolation_level);
//    Py_XSETREF(self->isolation_level, isolation_level);
//    return 0;
//}

PyObject* duckdb_connection_call(duckdb_Connection* self, PyObject* args, PyObject* kwargs)
{
    PyObject* sql;
    duckdb_Statement* statement;
    PyObject* weakref;
    int rc;

    if (!duckdb_check_thread(self) || !duckdb_check_connection(self)) {
        return NULL;
    }

    if (!_PyArg_NoKeywords(MODULE_NAME ".Connection", kwargs))
        return NULL;

    if (!PyArg_ParseTuple(args, "O", &sql))
        return NULL;

    _duckdb_drop_unused_statement_references(self);

    statement = PyObject_New(duckdb_Statement, &duckdb_StatementType);
    if (!statement) {
        return NULL;
    }

    statement->db = NULL;
    //statement->st = NULL;
    statement->sql = NULL;
    statement->in_use = 0;
    statement->in_weakreflist = NULL;

    rc = duckdb_statement_create(statement, self, sql);
    if (rc != DuckDBSuccess) {
        if (rc == duckdb_TOO_MUCH_SQL) {
            PyErr_SetString(duckdb_Warning, "You can only execute one statement at a time.");
        } else if (rc == duckdb_SQL_WRONG_TYPE) {
            if (PyErr_ExceptionMatches(PyExc_TypeError))
                PyErr_SetString(duckdb_Warning, "SQL is of wrong type. Must be string.");
        } else {
            (void)duckdb_statement_reset(statement);
//            _duckdb_seterror(self->db, NULL);
        }
        goto error;
    }

    weakref = PyWeakref_NewRef((PyObject*)statement, NULL);
    if (weakref == NULL)
        goto error;
    if (PyList_Append(self->statements, weakref) != 0) {
        Py_DECREF(weakref);
        goto error;
    }
    Py_DECREF(weakref);

    return (PyObject*)statement;

error:
    Py_DECREF(statement);
    return NULL;
}

PyObject* duckdb_connection_execute(duckdb_Connection* self, PyObject* args)
{
    PyObject* cursor = 0;
    PyObject* result = 0;
    PyObject* method = 0;

    cursor = _PyObject_CallMethodId((PyObject*)self, &PyId_cursor, NULL);
    if (!cursor) {
        goto error;
    }

    method = PyObject_GetAttrString(cursor, "execute");
    if (!method) {
        Py_CLEAR(cursor);
        goto error;
    }

    result = PyObject_CallObject(method, args);
    if (!result) {
        Py_CLEAR(cursor);
    }

error:
    Py_XDECREF(result);
    Py_XDECREF(method);

    return cursor;
}

PyObject* duckdb_connection_executemany(duckdb_Connection* self, PyObject* args)
{
    PyObject* cursor = 0;
    PyObject* result = 0;
    PyObject* method = 0;

    cursor = _PyObject_CallMethodId((PyObject*)self, &PyId_cursor, NULL);
    if (!cursor) {
        goto error;
    }

    method = PyObject_GetAttrString(cursor, "executemany");
    if (!method) {
        Py_CLEAR(cursor);
        goto error;
    }

    result = PyObject_CallObject(method, args);
    if (!result) {
        Py_CLEAR(cursor);
    }

error:
    Py_XDECREF(result);
    Py_XDECREF(method);

    return cursor;
}

PyObject* duckdb_connection_executescript(duckdb_Connection* self, PyObject* args)
{
    PyObject* cursor = 0;
    PyObject* result = 0;
    PyObject* method = 0;

    cursor = _PyObject_CallMethodId((PyObject*)self, &PyId_cursor, NULL);
    if (!cursor) {
        goto error;
    }

    method = PyObject_GetAttrString(cursor, "executescript");
    if (!method) {
        Py_CLEAR(cursor);
        goto error;
    }

    result = PyObject_CallObject(method, args);
    if (!result) {
        Py_CLEAR(cursor);
    }

error:
    Py_XDECREF(result);
    Py_XDECREF(method);

    return cursor;
}

/* ------------------------- COLLATION CODE ------------------------ */
//
//static int
//duckdb_collation_callback(
//        void* context,
//        int text1_length, const void* text1_data,
//        int text2_length, const void* text2_data)
//{
//    PyObject* callback = (PyObject*)context;
//    PyObject* string1 = 0;
//    PyObject* string2 = 0;
//    PyGILState_STATE gilstate;
//    PyObject* retval = NULL;
//    long longval;
//    int result = 0;
//    gilstate = PyGILState_Ensure();
//
//    if (PyErr_Occurred()) {
//        goto finally;
//    }
//
//    string1 = PyUnicode_FromStringAndSize((const char*)text1_data, text1_length);
//    string2 = PyUnicode_FromStringAndSize((const char*)text2_data, text2_length);
//
//    if (!string1 || !string2) {
//        goto finally; /* failed to allocate strings */
//    }
//
//    retval = PyObject_CallFunctionObjArgs(callback, string1, string2, NULL);
//
//    if (!retval) {
//        /* execution failed */
//        goto finally;
//    }
//
//    longval = PyLong_AsLongAndOverflow(retval, &result);
//    if (longval == -1 && PyErr_Occurred()) {
//        PyErr_Clear();
//        result = 0;
//    }
//    else if (!result) {
//        if (longval > 0)
//            result = 1;
//        else if (longval < 0)
//            result = -1;
//    }
//
//finally:
//    Py_XDECREF(string1);
//    Py_XDECREF(string2);
//    Py_XDECREF(retval);
//    PyGILState_Release(gilstate);
//    return result;
//}
//
//static PyObject *
//duckdb_connection_interrupt(duckdb_Connection* self, PyObject* args)
//{
//    PyObject* retval = NULL;
//
//    if (!duckdb_check_connection(self)) {
//        goto finally;
//    }
//
//    // FIXME export interrupt to C API
////    sqlite3_interrupt(self->db);
//
//    Py_INCREF(Py_None);
//    retval = Py_None;
//
//finally:
//    return retval;
//}


/* Called when the connection is used as a context manager. Returns itself as a
 * convenience to the caller. */
//static PyObject *
//duckdb_connection_enter(duckdb_Connection* self, PyObject* args)
//{
//    Py_INCREF(self);
//    return (PyObject*)self;
//}
//
///** Called when the connection is used as a context manager. If there was any
// * exception, a rollback takes place; otherwise we commit. */
//static PyObject *
//duckdb_connection_exit(duckdb_Connection* self, PyObject* args)
//{
//    PyObject* exc_type, *exc_value, *exc_tb;
//    const char* method_name;
//    PyObject* result;
//
//    if (!PyArg_ParseTuple(args, "OOO", &exc_type, &exc_value, &exc_tb)) {
//        return NULL;
//    }
//
//    if (exc_type == Py_None && exc_value == Py_None && exc_tb == Py_None) {
//        method_name = "commit";
//    } else {
//        method_name = "rollback";
//    }
//
//    result = PyObject_CallMethod((PyObject*)self, method_name, NULL);
//    if (!result) {
//        return NULL;
//    }
//    Py_DECREF(result);
//
//    Py_RETURN_FALSE;
//}

static const char connection_doc[] =
PyDoc_STR("SQLite database connection object.");

static PyGetSetDef connection_getset[] = {
//    {"isolation_level",  (getter)duckdb_connection_get_isolation_level, (setter)duckdb_connection_set_isolation_level},
//    {"total_changes",  (getter)duckdb_connection_get_total_changes, (setter)0},
//    {"in_transaction",  (getter)duckdb_connection_get_in_transaction, (setter)0},
    {NULL}
};

static PyMethodDef connection_methods[] = {
    {"cursor", (PyCFunction)(void(*)(void))duckdb_connection_cursor, METH_VARARGS|METH_KEYWORDS,
        PyDoc_STR("Return a cursor for the connection.")},
    {"close", (PyCFunction)duckdb_connection_close, METH_NOARGS,
        PyDoc_STR("Closes the connection.")},
    {"commit", (PyCFunction)duckdb_connection_commit, METH_NOARGS,
        PyDoc_STR("Commit the current transaction.")},
    {"rollback", (PyCFunction)duckdb_connection_rollback, METH_NOARGS,
        PyDoc_STR("Roll back the current transaction.")},
    {NULL, NULL}
};

static struct PyMemberDef connection_members[] =
{
    {"Warning", T_OBJECT, offsetof(duckdb_Connection, Warning), READONLY},
    {"Error", T_OBJECT, offsetof(duckdb_Connection, Error), READONLY},
//    {"InterfaceError", T_OBJECT, offsetof(duckdb_Connection, InterfaceError), READONLY},
//    {"DatabaseError", T_OBJECT, offsetof(duckdb_Connection, DatabaseError), READONLY},
//    {"DataError", T_OBJECT, offsetof(duckdb_Connection, DataError), READONLY},
//    {"OperationalError", T_OBJECT, offsetof(duckdb_Connection, OperationalError), READONLY},
//    {"IntegrityError", T_OBJECT, offsetof(duckdb_Connection, IntegrityError), READONLY},
//    {"InternalError", T_OBJECT, offsetof(duckdb_Connection, InternalError), READONLY},
//    {"ProgrammingError", T_OBJECT, offsetof(duckdb_Connection, ProgrammingError), READONLY},
//    {"NotSupportedError", T_OBJECT, offsetof(duckdb_Connection, NotSupportedError), READONLY},
    {NULL}
};

PyTypeObject duckdb_ConnectionType = {
        PyVarObject_HEAD_INIT(NULL, 0)
        MODULE_NAME ".Connection",                      /* tp_name */
        sizeof(duckdb_Connection),                    /* tp_basicsize */
        0,                                              /* tp_itemsize */
        (destructor)duckdb_connection_dealloc,        /* tp_dealloc */
        0,                                              /* tp_print */
        0,                                              /* tp_getattr */
        0,                                              /* tp_setattr */
        0,                                              /* tp_reserved */
        0,                                              /* tp_repr */
        0,                                              /* tp_as_number */
        0,                                              /* tp_as_sequence */
        0,                                              /* tp_as_mapping */
        0,                                              /* tp_hash */
        (ternaryfunc)duckdb_connection_call,          /* tp_call */
        0,                                              /* tp_str */
        0,                                              /* tp_getattro */
        0,                                              /* tp_setattro */
        0,                                              /* tp_as_buffer */
        Py_TPFLAGS_DEFAULT|Py_TPFLAGS_BASETYPE,         /* tp_flags */
        connection_doc,                                 /* tp_doc */
        0,                                              /* tp_traverse */
        0,                                              /* tp_clear */
        0,                                              /* tp_richcompare */
        0,                                              /* tp_weaklistoffset */
        0,                                              /* tp_iter */
        0,                                              /* tp_iternext */
        connection_methods,                             /* tp_methods */
        connection_members,                             /* tp_members */
        connection_getset,                              /* tp_getset */
        0,                                              /* tp_base */
        0,                                              /* tp_dict */
        0,                                              /* tp_descr_get */
        0,                                              /* tp_descr_set */
        0,                                              /* tp_dictoffset */
        (initproc)duckdb_connection_init,             /* tp_init */
        0,                                              /* tp_alloc */
        0,                                              /* tp_new */
        0                                               /* tp_free */
};

extern int duckdb_connection_setup_types(void)
{
    duckdb_ConnectionType.tp_new = PyType_GenericNew;
    return PyType_Ready(&duckdb_ConnectionType);
}

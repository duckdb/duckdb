/* connection.h - definitions for the connection type
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

#ifndef DUCKDB_CONNECTION_H
#define DUCKDB_CONNECTION_H
#include "Python.h"
#include "duckdb.hpp"
#include "module.h"
#include "pythread.h"
#include "structmember.h"

typedef struct {
	PyObject_HEAD std::unique_ptr<duckdb::DuckDB> db;
	std::unique_ptr<duckdb::Connection> conn;

	int initialized;

	/* Exception objects */
	PyObject *DatabaseError;
} duckdb_Connection;

extern PyTypeObject duckdb_ConnectionType;

PyObject *duckdb_connection_alloc(PyTypeObject *type, int aware);
void duckdb_connection_dealloc(duckdb_Connection *self);
PyObject *duckdb_connection_cursor(duckdb_Connection *self, PyObject *args, PyObject *kwargs);
PyObject *duckdb_connection_close(duckdb_Connection *self, PyObject *args);
PyObject *duckdb_connection_begin(duckdb_Connection *self);
PyObject *duckdb_connection_commit(duckdb_Connection *self);
PyObject *duckdb_connection_rollback(duckdb_Connection *self);
PyObject *duckdb_connection_new(PyTypeObject *type, PyObject *args, PyObject *kw);
int duckdb_connection_init(duckdb_Connection *self, PyObject *args, PyObject *kwargs);
int duckdb_check_connection(duckdb_Connection *con);

int duckdb_connection_setup_types(void);

#endif

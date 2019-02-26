/* statement.h - definitions for the statement type
 *
 * Copyright (C) 2005-2010 Gerhard Häring <gh@ghaering.de>
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

#ifndef DUCKDB_STATEMENT_H
#define DUCKDB_STATEMENT_H
#include "Python.h"

#include "connection.h"
#include "duckdb.h"

#define duckdb_TOO_MUCH_SQL (-100)
#define duckdb_SQL_WRONG_TYPE (-101)

typedef struct
{
    PyObject_HEAD
    duckdb_database* db;
//    sqlite3_stmt* st;
    PyObject* sql;
    int in_use;
    int is_dml;
    PyObject* in_weakreflist; /* List of weak references */
} duckdb_Statement;

extern PyTypeObject duckdb_StatementType;

int duckdb_statement_create(duckdb_Statement* self, duckdb_Connection* connection, PyObject* sql);
void duckdb_statement_dealloc(duckdb_Statement* self);

int duckdb_statement_bind_parameter(duckdb_Statement* self, int pos, PyObject* parameter);
void duckdb_statement_bind_parameters(duckdb_Statement* self, PyObject* parameters);

int duckdb_statement_finalize(duckdb_Statement* self);
int duckdb_statement_reset(duckdb_Statement* self);
void duckdb_statement_mark_dirty(duckdb_Statement* self);

int duckdb_statement_setup_types(void);

#endif

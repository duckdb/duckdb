/* module.h - definitions for the module
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

#ifndef DUCKDB_MODULE_H
#define DUCKDB_MODULE_H
#include "Python.h"
#include "duckdb.h"

typedef struct
{
    PyObject_HEAD
    duckdb_database* db;
    PyObject* in_weakreflist; /* List of weak references */
} duckdb_Database;

extern PyTypeObject duckdb_DatabaseType;


extern PyObject* duckdb_Error;
extern PyObject* duckdb_Warning;
extern PyObject* duckdb_InterfaceError;
extern PyObject* duckdb_DatabaseError;
extern PyObject* duckdb_InternalError;
extern PyObject* duckdb_OperationalError;
extern PyObject* duckdb_ProgrammingError;
extern PyObject* duckdb_IntegrityError;
extern PyObject* duckdb_DataError;
extern PyObject* duckdb_NotSupportedError;

//
//extern int _duckdb_enable_callback_tracebacks;
//extern int duckdb_BaseTypeAdapted;

#endif

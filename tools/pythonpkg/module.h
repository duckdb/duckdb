#ifndef DUCKDB_MODULE_H
#define DUCKDB_MODULE_H
#include "Python.h"
#include "duckdb.h"

typedef struct {
	PyObject_HEAD duckdb_database *db;
	PyObject *in_weakreflist; /* List of weak references */
} duckdb_Database;

extern PyTypeObject duckdb_DatabaseType;
extern PyObject *duckdb_DatabaseError;

#endif

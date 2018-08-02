//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// duckdb.h
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#ifdef __cplusplus
extern "C" {
#endif

typedef unsigned long long duckdb_oid_t;

typedef enum DUCKDB_TYPE {
	DUCKDB_TYPE_INVALID = 0,
	DUCKDB_TYPE_PARAMETER_OFFSET,
	DUCKDB_TYPE_BOOLEAN,
	DUCKDB_TYPE_TINYINT,
	DUCKDB_TYPE_SMALLINT,
	DUCKDB_TYPE_INTEGER,
	DUCKDB_TYPE_BIGINT,
	DUCKDB_TYPE_DECIMAL,
	DUCKDB_TYPE_POINTER,
	DUCKDB_TYPE_TIMESTAMP,
	DUCKDB_TYPE_DATE,
	DUCKDB_TYPE_VARCHAR,
	DUCKDB_TYPE_VARBINARY,
	DUCKDB_TYPE_ARRAY,
	DUCKDB_TYPE_UDT
} duckdb_type;

typedef struct {
	duckdb_type type;
	char *data;
	duckdb_oid_t count;
	char *name;
} duckdb_column;

typedef struct {
	duckdb_oid_t row_count;
	duckdb_oid_t column_count;
	duckdb_column *columns;
} duckdb_result;

typedef void *duckdb_database;
typedef void *duckdb_connection;

typedef enum  { DuckDBSuccess = 0, DuckDBError = 1 } duckdb_state;

duckdb_state duckdb_open(char *path, /* Database filename (UTF-8) */
                         duckdb_database *database /* OUT: DuckDB DB handle */
                         );

duckdb_state duckdb_close(duckdb_database database /* Database to close */
                          );

duckdb_state
duckdb_connect(duckdb_database database, /* Database to open connection to */
               duckdb_connection *connection /* OUT: Connection handle */
               );

duckdb_state
duckdb_disconnect(duckdb_connection connection /* Connection handle */
                  );

duckdb_state
duckdb_query(duckdb_connection connection, /* Connection to query */
             const char *query,            /* SQL query to execute */
             duckdb_result *result         /* OUT: query result */
             );

void duckdb_print_result(duckdb_result result /* The result to print */
                         );

void duckdb_destroy_result(duckdb_result result /* The result to destroy */
                           );

#ifdef __cplusplus
};
#endif

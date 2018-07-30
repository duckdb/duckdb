// C Interface to DuckDB

#pragma once

#ifdef __cplusplus
extern "C" {
#endif

typedef void *duckdb_database;
typedef void *duckdb_connection;
typedef void *duckdb_result;

enum duckdb_state { DuckDBSuccess = 0, DuckDBError = 1 };

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

#ifdef __cplusplus
};
#endif

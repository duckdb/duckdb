#include "duckdb.h"
#include <stdio.h>

int main() {
	duckdb_database db = NULL;
	duckdb_connection con = NULL;
	duckdb_result result;

	if (duckdb_open(NULL, &db) == DuckDBError) {
		fprintf(stderr, "Failed to open database\n");
		goto cleanup;
	}
	if (duckdb_connect(db, &con) == DuckDBError) {
		fprintf(stderr, "Failed to open connection\n");
		goto cleanup;
	}
	if (duckdb_query(con, "CREATE TABLE integers(i INTEGER)", NULL) == DuckDBError) {
		fprintf(stderr, "Failed to query database\n");
		goto cleanup;
	}
	if (duckdb_query(con, "INSERT INTO integers VALUES (3)", NULL) == DuckDBError) {
		fprintf(stderr, "Failed to query database\n");
		goto cleanup;
	}
	if (duckdb_query(con, "SELECT * FROM integers", &result) == DuckDBError) {
		fprintf(stderr, "Failed to query database\n");
		goto cleanup;
	}
	duckdb_print_result(result);
cleanup:
	duckdb_destroy_result(result);
	duckdb_disconnect(con);
	duckdb_close(db);
}

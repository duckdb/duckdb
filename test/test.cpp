
#include <stdlib.h>
#include <stdio.h>

#include "duckdb.h"

int main() {
	duckdb database;
	duckdb_connection connection;
	duckdb_result result;

	if (duckdb_open(NULL, &database) != DuckDBSuccess) {
		fprintf(stderr, "Database startup failed!\n");
		return 1;
	}

	if (duckdb_connect(database, &connection) != DuckDBSuccess) {
		fprintf(stderr, "Database connection failed!\n");
		return 1;
	}

	if (duckdb_query(connection, "SELECT 42;", &result) != DuckDBSuccess) {
		fprintf(stderr, "Database query failed!\n");
		return 1;
	}

	if (duckdb_close(database) != DuckDBSuccess) {
		fprintf(stderr, "Database exit failed!\n");
		return 1;
	}
	return 0;
}

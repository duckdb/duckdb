
#include <stdio.h>
#include <stdlib.h>

#include "duckdb.h"

void execute(duckdb_connection connection, const char *query) {
	duckdb_result result;

	printf("%s\n", query);
	if (duckdb_query(connection, query, &result) != DuckDBSuccess) {
		printf("Failure!\n");
		exit(1);
	}

	duckdb_print_result(result);
	duckdb_destroy_result(result);
}

#define EXEC(query) execute(connection, query)

int main() {
	duckdb_database database;
	duckdb_connection connection;

	if (duckdb_open((char *)0, &database) != DuckDBSuccess) {
		fprintf(stderr, "Database startup failed!\n");
		return 1;
	}

	if (duckdb_connect(database, &connection) != DuckDBSuccess) {
		fprintf(stderr, "Database connection failed!\n");
		return 1;
	}

	EXEC("SELECT 42");

	if (duckdb_disconnect(connection) != DuckDBSuccess) {
		fprintf(stderr, "Database exit failed!\n");
		return 1;
	}
	if (duckdb_close(database) != DuckDBSuccess) {
		fprintf(stderr, "Database exit failed!\n");
		return 1;
	}
	return 0;
}

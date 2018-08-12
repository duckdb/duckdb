
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

	EXEC("CREATE TABLE test (a INTEGER, b INTEGER);");
	EXEC("INSERT INTO test VALUES (11, 1)");
	EXEC("INSERT INTO test VALUES (12, 2)");
	EXEC("INSERT INTO test VALUES (13, 3)");
	EXEC("CREATE TABLE test2 (b INTEGER, c INTEGER);");
	EXEC("INSERT INTO test2 VALUES (1, 10)");
	EXEC("INSERT INTO test2 VALUES (1, 20)");
	EXEC("INSERT INTO test2 VALUES (2, 30)");
	EXEC("SELECT a, test.b, c FROM test, test2 WHERE test.b = test2.b;");

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

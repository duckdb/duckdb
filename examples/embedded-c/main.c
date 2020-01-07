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
	if (duckdb_query(con, "CREATE TABLE integers(i INTEGER, j INTEGER);", NULL) == DuckDBError) {
		fprintf(stderr, "Failed to query database\n");
		goto cleanup;
	}
	if (duckdb_query(con, "INSERT INTO integers VALUES (3, 4), (5, 6), (7, NULL);", NULL) == DuckDBError) {
		fprintf(stderr, "Failed to query database\n");
		goto cleanup;
	}
	if (duckdb_query(con, "SELECT * FROM integers", &result) == DuckDBError) {
		fprintf(stderr, "Failed to query database\n");
		goto cleanup;
	}
	// print the names of the result
	for (size_t i = 0; i < result.column_count; i++) {
		printf("%s ", result.columns[i].name);
	}
	printf("\n");
	// print the data of the result
	for (size_t row_idx = 0; row_idx < result.row_count; row_idx++) {
		for (size_t col_idx = 0; col_idx < result.column_count; col_idx++) {
			char *val = duckdb_value_varchar(&result, col_idx, row_idx);
			printf("%s ", val);
			free(val);
		}
		printf("\n");
	}
	// duckdb_print_result(result);
cleanup:
	duckdb_destroy_result(&result);
	duckdb_disconnect(&con);
	duckdb_close(&db);
}

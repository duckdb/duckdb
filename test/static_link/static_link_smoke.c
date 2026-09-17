// Links the parquet archive and names only its root: the root member has to register parquet with the engine. Exits
// non-zero if parquet is not loaded.
#include "duckdb.h"

#include <stdio.h>
#include <string.h>

int main(void) {
	duckdb_database db;
	duckdb_connection con;
	duckdb_result res;
	if (duckdb_open(NULL, &db) != DuckDBSuccess || duckdb_connect(db, &con) != DuckDBSuccess) {
		fprintf(stderr, "could not open an in-memory database\n");
		return 1;
	}
	if (duckdb_query(con, "SELECT extension_name FROM duckdb_extensions() WHERE loaded ORDER BY 1", &res) !=
	    DuckDBSuccess) {
		fprintf(stderr, "query failed: %s\n", duckdb_result_error(&res));
		return 1;
	}
	int found = 0;
	for (idx_t i = 0; i < duckdb_row_count(&res); i++) {
		char *name = duckdb_value_varchar(&res, 0, i);
		printf("loaded: %s\n", name);
		if (strcmp(name, "parquet") == 0) {
			found = 1;
		}
		duckdb_free(name);
	}
	duckdb_destroy_result(&res);
	duckdb_disconnect(&con);
	duckdb_close(&db);
	if (!found) {
		fprintf(stderr, "parquet was on the link line but is not loaded: its root member did not register it\n");
		return 1;
	}
	return 0;
}


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

	if (duckdb_query(connection, "SELECT 42, 'hello';", &result) != DuckDBSuccess) {
		fprintf(stderr, "Database query failed!\n");
		return 1;
	}

	if (duckdb_query(connection, "SELECT id FROM tbl;", &result) != DuckDBSuccess) {
		fprintf(stderr, "Database query failed!\n");
		return 1;
	}

	if (duckdb_query(connection, "SELECT id, id + 1 FROM tbl;", &result) != DuckDBSuccess) {
		fprintf(stderr, "Database query failed!\n");
		return 1;
	}

	if (duckdb_query(connection, "select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order from lineitem where l_shipdate <= '1998-09-02' group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus;", &result) != DuckDBSuccess) {
		fprintf(stderr, "Database query failed!\n");
		return 1;
	}

	if (duckdb_close(database) != DuckDBSuccess) {
		fprintf(stderr, "Database exit failed!\n");
		return 1;
	}
	return 0;
}

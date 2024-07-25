#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/main/config.hpp"

using namespace duckdb;
using namespace std;

void rand_str(char *dest, idx_t length) {
	char charset[] = "0123456789"
	                 "abcdefghijklmnopqrstuvwxyz"
	                 "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

	while (length-- > 0) {
		idx_t index = (double)rand() / RAND_MAX * (sizeof charset - 1);
		*dest++ = charset[index];
	}
	*dest = '\0';
}

TEST_CASE("Test repeated appending small chunks to a table", "[memoryleak]") {
	if (!TestMemoryLeaks()) {
		return;
	}
	duckdb_database db;
	duckdb_connection con;
	duckdb_state state;
	auto db_path = TestCreatePath("appender_leak_test.db");
	TestDeleteFile(db_path);

	if (duckdb_open(db_path.c_str(), &db) == DuckDBError) {
		// handle error
		FAIL("Failed to open");
	}
	if (duckdb_connect(db, &con) == DuckDBError) {
		// handle error
		FAIL("Failed to connect");
	}

	state =
	    duckdb_query(con, "create table test(col1 varchar, col2 varchar, col3 bigint, col4 bigint, col5 double)", NULL);
	if (state == DuckDBError) {
		FAIL("Failed to create table");
	}
	state = duckdb_query(con, "set memory_limit='100mb'", NULL);
	if (state == DuckDBError) {
		FAIL("Failed to set memory limit");
	}

	long n1 = 0;
	double d1 = 0.5;
	for (int i = 0; i < 100000; i++) {
		duckdb_appender appender;
		if (duckdb_appender_create(con, NULL, "test", &appender) == DuckDBError) {
			FAIL("Failed to create appender");
		}
		for (int j = 0; j < 1000; j++) {
			char str[41];
			rand_str(str, sizeof(str) - 1);
			duckdb_append_varchar(appender, str);
			duckdb_append_varchar(appender, "hello");
			duckdb_append_int64(appender, n1++);
			duckdb_append_int64(appender, n1++);
			duckdb_append_double(appender, d1);
			d1 += 1.25;
			duckdb_appender_end_row(appender);
		}
		state = duckdb_appender_close(appender);
		if (state == DuckDBError) {
			FAIL("Failed to close appender");
		}
		state = duckdb_appender_destroy(&appender);
		if (state == DuckDBError) {
			fprintf(stderr, "err: %d", state);
			FAIL("Failed to destroy appender");
		}
		if (i % 500 == 0) {
			printf("completed %d\n", i);
			duckdb_query(con, "checkpoint", NULL);
		}
	}

	// cleanup
	duckdb_disconnect(&con);
	duckdb_close(&db);
	REQUIRE(1 == 1);
}

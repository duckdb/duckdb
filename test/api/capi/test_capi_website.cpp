#include "capi_tester.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test C API examples from the website", "[capi]") {
	// NOTE: if any of these break and need to be changed, the website also needs to be updated!
	SECTION("connect") {
		duckdb_database db;
		duckdb_connection con;

		if (duckdb_open(NULL, &db) == DuckDBError) {
			// handle error
		}
		if (duckdb_connect(db, &con) == DuckDBError) {
			// handle error
		}

		// run queries...

		// cleanup
		duckdb_disconnect(&con);
		duckdb_close(&db);
	}
	SECTION("config") {
		duckdb_database db;
		duckdb_config config;

		// create the configuration object
		if (duckdb_create_config(&config) == DuckDBError) {
			REQUIRE(1 == 0);
		}
		// set some configuration options
		duckdb_set_config(config, "access_mode", "READ_WRITE");
		duckdb_set_config(config, "threads", "8");
		duckdb_set_config(config, "max_memory", "8GB");
		duckdb_set_config(config, "default_order", "DESC");

		// open the database using the configuration
		if (duckdb_open_ext(NULL, &db, config, NULL) == DuckDBError) {
			REQUIRE(1 == 0);
		}
		// cleanup the configuration object
		duckdb_destroy_config(&config);

		// run queries...

		// cleanup
		duckdb_close(&db);
	}
	SECTION("query") {
		duckdb_database db;
		duckdb_connection con;
		duckdb_state state;
		duckdb_result result;

		duckdb_open(NULL, &db);
		duckdb_connect(db, &con);

		// create a table
		state = duckdb_query(con, "CREATE TABLE integers(i INTEGER, j INTEGER);", NULL);
		if (state == DuckDBError) {
			REQUIRE(1 == 0);
		}
		// insert three rows into the table
		state = duckdb_query(con, "INSERT INTO integers VALUES (3, 4), (5, 6), (7, NULL);", NULL);
		if (state == DuckDBError) {
			REQUIRE(1 == 0);
		}
		// query rows again
		state = duckdb_query(con, "SELECT * FROM integers", &result);
		if (state == DuckDBError) {
			REQUIRE(1 == 0);
		}
		// handle the result
		idx_t row_count = duckdb_row_count(&result);
		idx_t column_count = duckdb_column_count(&result);
		for (idx_t row = 0; row < row_count; row++) {
			for (idx_t col = 0; col < column_count; col++) {
				// if (col > 0) printf(",");
				auto str_val = duckdb_value_varchar(&result, col, row);
				// printf("%s", str_val);
				REQUIRE(1 == 1);
				duckdb_free(str_val);
			}
			//	printf("\n");
		}

		int32_t *i_data = (int32_t *)duckdb_column_data(&result, 0);
		int32_t *j_data = (int32_t *)duckdb_column_data(&result, 1);
		bool *i_mask = duckdb_nullmask_data(&result, 0);
		bool *j_mask = duckdb_nullmask_data(&result, 1);
		for (idx_t row = 0; row < row_count; row++) {
			if (i_mask[row]) {
				// printf("NULL");
			} else {
				REQUIRE(i_data[row] > 0);
				// printf("%d", i_data[row]);
			}
			// printf(",");
			if (j_mask[row]) {
				// printf("NULL");
			} else {
				REQUIRE(j_data[row] > 0);
				// printf("%d", j_data[row]);
			}
			// printf("\n");
		}

		// destroy the result after we are done with it
		duckdb_destroy_result(&result);
		duckdb_disconnect(&con);
		duckdb_close(&db);
	}
	SECTION("prepared") {
		duckdb_database db;
		duckdb_connection con;
		duckdb_open(NULL, &db);
		duckdb_connect(db, &con);
		duckdb_query(con, "CREATE TABLE integers(i INTEGER, j INTEGER)", NULL);

		duckdb_prepared_statement stmt;
		duckdb_result result;
		if (duckdb_prepare(con, "INSERT INTO integers VALUES ($1, $2)", &stmt) == DuckDBError) {
			REQUIRE(1 == 0);
		}

		duckdb_bind_int32(stmt, 1, 42); // the parameter index starts counting at 1!
		duckdb_bind_int32(stmt, 2, 43);
		// NULL as second parameter means no result set is requested
		duckdb_execute_prepared(stmt, NULL);
		duckdb_destroy_prepare(&stmt);

		// we can also query result sets using prepared statements
		if (duckdb_prepare(con, "SELECT * FROM integers WHERE i = ?", &stmt) == DuckDBError) {
			REQUIRE(1 == 0);
		}
		duckdb_bind_int32(stmt, 1, 42);
		duckdb_execute_prepared(stmt, &result);

		// do something with result

		// clean up
		duckdb_destroy_result(&result);
		duckdb_destroy_prepare(&stmt);

		duckdb_disconnect(&con);
		duckdb_close(&db);
	}
	SECTION("appender") {
		duckdb_database db;
		duckdb_connection con;
		duckdb_open(NULL, &db);
		duckdb_connect(db, &con);
		duckdb_query(con, "CREATE TABLE people(id INTEGER, name VARCHAR)", NULL);

		duckdb_appender appender;
		if (duckdb_appender_create(con, NULL, "people", &appender) == DuckDBError) {
			REQUIRE(1 == 0);
		}
		duckdb_append_int32(appender, 1);
		duckdb_append_varchar(appender, "Mark");
		duckdb_appender_end_row(appender);

		duckdb_append_int32(appender, 2);
		duckdb_append_varchar(appender, "Hannes");
		duckdb_appender_end_row(appender);

		duckdb_appender_destroy(&appender);

		duckdb_result result;
		duckdb_query(con, "SELECT * FROM people", &result);
		REQUIRE(duckdb_value_int32(&result, 0, 0) == 1);
		REQUIRE(duckdb_value_int32(&result, 0, 1) == 2);
		REQUIRE(string(duckdb_value_varchar_internal(&result, 1, 0)) == "Mark");
		REQUIRE(string(duckdb_value_varchar_internal(&result, 1, 1)) == "Hannes");

		// error conditions: we cannot
		REQUIRE(duckdb_value_varchar_internal(&result, 0, 0) == nullptr);
		REQUIRE(duckdb_value_varchar_internal(nullptr, 0, 0) == nullptr);

		duckdb_destroy_result(&result);

		duckdb_disconnect(&con);
		duckdb_close(&db);
	}
}

#include "capi_tester.hpp"
#include "cbuffer_manager_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test C API CBufferManager", "[capi]") {
	duckdb_database db;
	duckdb_config config;

	// create the configuration object
	if (duckdb_create_config(&config) == DuckDBError) {
		REQUIRE(1 == 0);
	}
	MyBufferManager external_manager;

	// set some configuration options
	if (duckdb_add_custom_buffer_manager(config, &external_manager, Allocate, ReAllocate, Destroy, Pin, Unpin,
	                                     MaxMemory, UsedMemory) != DuckDBSuccess) {
		REQUIRE(1 == 0);
	}

	// open the database using the configuration
	if (duckdb_open_ext(NULL, &db, config, NULL) == DuckDBError) {
		REQUIRE(1 == 0);
	}
	// cleanup the configuration object
	duckdb_destroy_config(&config);

	duckdb_connection connection;
	if (duckdb_connect(db, &connection) != DuckDBSuccess) {
		REQUIRE(1 == 0);
	}

	duckdb_result result;
	// Set to single threaded because the dummy buffer manager does not support multi threaded operations
	if (duckdb_query(connection, "pragma threads=1", &result) != DuckDBSuccess) {
		REQUIRE(1 == 0);
	}
	duckdb_destroy_result(&result);

	// run queries...
	if (duckdb_query(connection, "create table tbl as select * from range(1000000)", &result) != DuckDBSuccess) {
		REQUIRE(1 == 0);
	}
	duckdb_destroy_result(&result);
	if (duckdb_query(connection, "select * from tbl", &result) != DuckDBSuccess) {
		REQUIRE(1 == 0);
	}
	for (idx_t i = 0; i < 1000000; i++) {
		REQUIRE(duckdb_value_int64(&result, 0, i) == (int64_t)i);
	}
	// cleanup
	duckdb_destroy_result(&result);
	duckdb_disconnect(&connection);
	duckdb_close(&db);
}

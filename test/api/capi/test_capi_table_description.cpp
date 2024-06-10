#include "capi_tester.hpp"
#include "duckdb.h"

using namespace duckdb;
using namespace std;

TEST_CASE("Test table description in C API", "[capi]") {
	CAPITester tester;
	duckdb_state status;
	duckdb_table_description table_description = nullptr;

	// open the database in in-memory mode
	REQUIRE(tester.OpenDatabase(nullptr));

	// Table doesn't exist yet
	status = duckdb_table_description_create(tester.connection, nullptr, "test", &table_description);
	REQUIRE(status == DuckDBError);
	duckdb_table_description_destroy(&table_description);

	tester.Query("CREATE TABLE test (i integer, j integer default 5)");

	// The table was not created in this schema
	status = duckdb_table_description_create(tester.connection, "non-existant", "test", &table_description);
	REQUIRE(status == DuckDBError);
	duckdb_table_description_destroy(&table_description);

	status = duckdb_table_description_create(tester.connection, nullptr, "test", &table_description);
	REQUIRE(status == DuckDBSuccess);
	REQUIRE(duckdb_table_description_error(table_description) == nullptr);

	bool has_default;
	SECTION("Out of range column") {
		status = duckdb_column_has_default(table_description, 2, &has_default);
		REQUIRE(status == DuckDBError);
	}
	SECTION("In range column - not default") {
		status = duckdb_column_has_default(table_description, 0, &has_default);
		REQUIRE(status == DuckDBSuccess);
		REQUIRE(has_default == false);
	}
	SECTION("In range column - default") {
		status = duckdb_column_has_default(table_description, 1, &has_default);
		REQUIRE(status == DuckDBSuccess);
		REQUIRE(has_default == true);
	}
	duckdb_table_description_destroy(&table_description);
}

#include "capi_tester.hpp"
#include "duckdb.h"

using namespace duckdb;
using namespace std;

TEST_CASE("Test table description in C API", "[capi]") {
	CAPITester tester;
	duckdb_state status;

	// open the database in in-memory mode
	REQUIRE(tester.OpenDatabase(nullptr));

	tester.Query("CREATE TABLE test (i integer, j integer default 5)");
	duckdb_table_description table_description;

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

	status = duckdb_table_description_destroy(&table_description);
	REQUIRE(status == DuckDBSuccess);
}

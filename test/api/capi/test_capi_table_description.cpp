#include "capi_tester.hpp"
#include "duckdb.h"

using namespace duckdb;
using namespace std;

TEST_CASE("Test the table description in the C API", "[capi]") {
	CAPITester tester;
	REQUIRE(tester.OpenDatabase(nullptr));
	duckdb_table_description table_description = nullptr;
	tester.Query("SET threads=1;");

	// Test a non-existent table.
	auto status = duckdb_table_description_create(tester.connection, nullptr, "test", &table_description);
	REQUIRE(status == DuckDBError);
	duckdb_table_description_destroy(&table_description);

	status = duckdb_table_description_create_ext(tester.connection, "hello", "world", "test", &table_description);
	REQUIRE(status == DuckDBError);
	duckdb_table_description_destroy(&table_description);

	// Create an in-memory table and a table in an external file.
	tester.Query("CREATE TABLE test (i INTEGER, j INTEGER default 5)");
	auto test_dir = TestDirectoryPath();
	auto attach_query = "ATTACH '" + test_dir + "/ext_description.db'";
	tester.Query(attach_query);
	tester.Query("CREATE TABLE ext_description.test(my_column INTEGER)");

	// Test invalid catalog and schema.
	status =
	    duckdb_table_description_create_ext(tester.connection, "non-existent", nullptr, "test", &table_description);
	REQUIRE(status == DuckDBError);
	duckdb_table_description_destroy(&table_description);

	status = duckdb_table_description_create(tester.connection, "non-existent", "test", &table_description);
	REQUIRE(status == DuckDBError);
	duckdb_table_description_destroy(&table_description);

	status = duckdb_table_description_create(tester.connection, nullptr, "test", &table_description);
	REQUIRE(status == DuckDBSuccess);
	REQUIRE(duckdb_table_description_error(table_description) == nullptr);

	bool has_default;
	SECTION("Passing nullptr to has_default") {
		REQUIRE(duckdb_column_has_default(table_description, 2, nullptr) == DuckDBError);
		REQUIRE(duckdb_column_has_default(nullptr, 2, &has_default) == DuckDBError);
	}
	SECTION("Out of range column for has_default") {
		REQUIRE(duckdb_column_has_default(table_description, 2, &has_default) == DuckDBError);
	}
	SECTION("In range column - not default") {
		REQUIRE(duckdb_column_has_default(table_description, 0, &has_default) == DuckDBSuccess);
		REQUIRE(has_default == false);
	}
	SECTION("In range column - default") {
		REQUIRE(duckdb_column_has_default(table_description, 1, &has_default) == DuckDBSuccess);
		REQUIRE(has_default == true);
	}
	duckdb_table_description_destroy(&table_description);

	// Let's get information about the external table.
	status =
	    duckdb_table_description_create_ext(tester.connection, "ext_description", nullptr, "test", &table_description);
	REQUIRE(status == DuckDBSuccess);
	REQUIRE(duckdb_table_description_error(table_description) == nullptr);

	SECTION("Passing nullptr to get_name") {
		REQUIRE(duckdb_table_description_get_column_name(nullptr, 0) == nullptr);
	}
	SECTION("Out of range column for get_name") {
		REQUIRE(duckdb_table_description_get_column_name(table_description, 1) == nullptr);
	}
	SECTION("In range column - get the name") {
		auto column_name = duckdb_table_description_get_column_name(table_description, 0);
		string expected = "my_column";
		REQUIRE(!expected.compare(column_name));
		duckdb_free(column_name);
	}
	duckdb_table_description_destroy(&table_description);
}

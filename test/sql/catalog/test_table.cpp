#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test failure cases in table creation/deletion", "[catalog]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);

	// primary key constraint that references unknown column
	REQUIRE_FAIL(con.Query("CREATE TABLE integers(i INTEGER, PRIMARY KEY(j))"));
	// primary key that references the same key twice
	REQUIRE_FAIL(con.Query("CREATE TABLE integers(i INTEGER, PRIMARY KEY(i, i))"));
	// multiple primary keys
	REQUIRE_FAIL(con.Query("CREATE TABLE integers(i INTEGER, PRIMARY KEY(i), PRIMARY KEY(i)"));
	REQUIRE_FAIL(con.Query("CREATE TABLE integers(i INTEGER PRIMARY KEY, PRIMARY KEY(i)"));
}

TEST_CASE("Test temporary table creation", "[catalog]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);

	// temp tables are not supported yet
	REQUIRE_FAIL(con.Query("CREATE TEMPORARY TABLE integers(i INTEGER)"));
}

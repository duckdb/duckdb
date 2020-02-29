#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test failure cases in table creation/deletion", "[catalog]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// primary key constraint that references unknown column
	REQUIRE_FAIL(con.Query("CREATE TABLE integers(i INTEGER, PRIMARY KEY(j))"));
	// primary key that references the same key twice
	REQUIRE_FAIL(con.Query("CREATE TABLE integers(i INTEGER, PRIMARY KEY(i, i))"));
	// multiple primary keys
	REQUIRE_FAIL(con.Query("CREATE TABLE integers(i INTEGER, PRIMARY KEY(i), PRIMARY KEY(i)"));
	REQUIRE_FAIL(con.Query("CREATE TABLE integers(i INTEGER PRIMARY KEY, PRIMARY KEY(i)"));
}

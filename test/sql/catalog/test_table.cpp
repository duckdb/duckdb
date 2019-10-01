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

TEST_CASE("Test temporary table creation", "[catalog]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TEMPORARY TABLE integers(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (42)"));
	result = con.Query("SELECT i from integers");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
}


// todo temp tables survive commit but not rollback
// todo on commit preserve rows default
// todo temp tables override normal tables (?)
// todo temp tables create/delete/alter/contents are not persisted nor logged

#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("CHECK constraint", "[constraints]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER CHECK(i < 5))"));
	// insert value that passes
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (3)"));
	// insert value that doesn't pass
	REQUIRE_FAIL(con.Query("INSERT INTO integers VALUES (7)"));
	// insert NULL
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (NULL)"));

	result = con.Query("SELECT * FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {3, Value()}));

	REQUIRE_NO_FAIL(con.Query("DROP TABLE integers;"));

	// constraint on multiple columns
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER CHECK(i + j < 10), j INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (3, 3)"));
	// some values pass some values don't
	REQUIRE_FAIL(con.Query("INSERT INTO integers VALUES (5, 5)"));
	REQUIRE_FAIL(con.Query("INSERT INTO integers VALUES (3, 3), (5, 5)"));

	result = con.Query("SELECT * FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));
	REQUIRE(CHECK_COLUMN(result, 1, {3}));

	// subquery not allowed in CHECK
	REQUIRE_FAIL(con.Query("CREATE TABLE integers2(i INTEGER CHECK(i > (SELECT 42)), j INTEGER)"));
	// aggregate not allowed in CHECK
	REQUIRE_FAIL(con.Query("CREATE TABLE integers2(i INTEGER CHECK(i > SUM(j)), j INTEGER)"));
}

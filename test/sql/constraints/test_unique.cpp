#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Single UNIQUE constraint", "[constraints]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER UNIQUE, j INTEGER)"));

	// insert unique values
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (3, 4), (2, 5)"));

	result = con.Query("SELECT * FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {3, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {4, 5}));

	// insert a duplicate value as part of a chain of values, this should fail
	REQUIRE_FAIL(con.Query("INSERT INTO integers VALUES (6, 6), (3, 4);"));

	// unique constraints accept NULL values, unlike PRIMARY KEY columns
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (NULL, 3), (NULL, 3)"));

	// but if we try to replace them like this it's going to fail
	REQUIRE_FAIL(con.Query("UPDATE integers SET i=77 WHERE i IS NULL"));

	result = con.Query("SELECT * FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), Value(), 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {3, 3, 5, 4}));

	// multiple constraints: PRIMARY KEY and UNIQUE
	REQUIRE_NO_FAIL(con.Query("DROP TABLE integers"));
}

TEST_CASE("Multiple constraints", "[constraints]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER PRIMARY KEY, j INTEGER UNIQUE)"));

	// no constraints are violated
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1, 1), (2, 2)"));
	// only the second UNIQUE constraint is violated
	REQUIRE_FAIL(con.Query("INSERT INTO integers VALUES (3, 3), (4, 1)"));
	// no constraints are violated
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (3, 3), (4, 4)"));

	result = con.Query("SELECT * FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3, 4}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 2, 3, 4}));
}

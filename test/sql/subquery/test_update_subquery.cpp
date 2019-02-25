#include "catch.hpp"
#include "common/file_system.hpp"
#include "dbgen.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test subqueries in update", "[subquery]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);

	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(id INTEGER, i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1, 1), (2, 2), (3, 3), (4, NULL)"));

	// subquery in update
	REQUIRE_NO_FAIL(con.Query("UPDATE integers i1 SET i=(SELECT MAX(i) FROM integers WHERE i1.i<>i)"));

	result = con.Query("SELECT id, i FROM integers ORDER BY id");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3, 4}));
	REQUIRE(CHECK_COLUMN(result, 1, {3, 3, 2, Value()}));

	// subquery in delete
	// result = con.Query("DELETE FROM integers i1 WHERE i=(SELECT MAX(i) FROM integers WHERE i1.i=i)");
	// REQUIRE(CHECK_COLUMN(result, 0, {3}));

	// result = con.Query("SELECT id, i FROM integers ORDER BY id");
	// REQUIRE(CHECK_COLUMN(result, 0, {4}));
	// REQUIRE(CHECK_COLUMN(result, 1, {Value()}));
}

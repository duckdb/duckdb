#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test conjunction statements", "[conjunction]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);

	// create table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE a (i integer, j integer);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO a VALUES (3, 4), (4, 5), (5, 6)"));

	result = con.Query("SELECT * FROM a WHERE (i > 3 AND j < 5) OR (i > 3 AND j > 5)");
	REQUIRE(CHECK_COLUMN(result, 0, {5}));
	REQUIRE(CHECK_COLUMN(result, 1, {6}));
}

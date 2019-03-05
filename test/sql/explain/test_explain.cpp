#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test explain", "[explain]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);
	con.EnableQueryVerification();
	con.EnableProfiling();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER, j INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1, 1), (2, 2), (3, 3), (NULL, NULL)"));

	REQUIRE_NO_FAIL(con.Query("EXPLAIN SELECT * FROM integers"));
	REQUIRE_NO_FAIL(con.Query("EXPLAIN select sum(i), j, sum(i), j from integers group by j having j < 10;"));
}

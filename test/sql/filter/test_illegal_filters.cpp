#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test aggregation in WHERE", "[filter]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(a INTEGER, b INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (2, 12)"));

	REQUIRE_FAIL(con.Query("SELECT * FROM integers WHERE SUM(a)>10"));
}

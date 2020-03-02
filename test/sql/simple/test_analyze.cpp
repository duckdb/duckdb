#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test ANALYZE", "[analyze]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	// ANALYZE runs without errors, note that ANALYZE is actually just isgnored
	REQUIRE_NO_FAIL(con.Query("ANALYZE"));
}

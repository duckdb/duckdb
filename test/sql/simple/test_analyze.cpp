#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test ANALYZE", "[analyze]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	// ANALYZE runs without errors, note that ANALYZE is actually just ignored
	REQUIRE_NO_FAIL(con.Query("ANALYZE"));
	REQUIRE_NO_FAIL(con.Query("VACUUM"));

	auto prep = con.Prepare("ANALYZE");
	REQUIRE(prep->success);
	auto res = prep->Execute();
	REQUIRE(res->success);

	prep = con.Prepare("VACUUM");
	REQUIRE(prep->success);
	res = prep->Execute();
	REQUIRE(res->success);
}

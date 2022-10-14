#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Streaming result with filter operation applies operator caching", "[api][.]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test as SELECT i FROM range(0,100000) tbl(i)"));
	// now create a streaming result
	auto result = con.SendQuery("SELECT * FROM test where i % 20000 = 0");
	REQUIRE_NO_FAIL(*result);
	// initial query does not fail!
	auto chunk = result->Fetch();
	REQUIRE(chunk);
	// the chunk should contain all 5 values if chunk caching is applied correctly
	REQUIRE(chunk->size() == 5);
}

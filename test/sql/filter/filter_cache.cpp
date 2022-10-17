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

// This test triggers an edge case where data that is flushed from a caching operator in pulled into an operator
// that needs to see this chunk more that once. handling this case requires temporarily caching the flushed result.
TEST_CASE("Streaming result with a filter and a cross product", "[api][.]") {
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("PRAGMA disable_optimizer"));

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test as SELECT * FROM RANGE(0, 2000000) tbl(i)"));
	auto result = con.SendQuery("SELECT * FROM (SELECT * FROM test where i%1000=0) t1(i), (SELECT * FROM test where i%1000=0) t2(j)");
	REQUIRE_NO_FAIL(*result);

	idx_t expected_count = 4000000;
	idx_t got_count = 0;

	while(true) {
		auto chunk = result->Fetch();
		if (chunk) {
			got_count += chunk->size();
		} else {
			break;
		}
	}

	REQUIRE(got_count == expected_count);
}
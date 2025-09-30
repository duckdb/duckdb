#include "catch.hpp"
#include "duckdb/common/assert.hpp"

TEST_CASE("Assertion passes", "[assertion]") {
	DUCKDB_ASSERT(true);
}

TEST_CASE("Assertion fails", "[assertion]") {
	REQUIRE_THROWS(DUCKDB_ASSERT(false));
}

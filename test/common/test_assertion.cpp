#include "catch.hpp"
#include "duckdb/common/assert.hpp"

TEST_CASE("Assertion passes", "[assertion]") {
	ALWAYS_ASSERT(true);
}

#ifndef DUCKDB_CRASH_ON_ASSERT
TEST_CASE("Assertion fails", "[assertion]") {
	REQUIRE_THROWS(ALWAYS_ASSERT(false));
}
#endif

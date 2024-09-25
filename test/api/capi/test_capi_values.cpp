#include "capi_tester.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test MAP getters", "[capi]") {
	auto uint_val = duckdb_create_uint64(42);
	REQUIRE(uint_val);

	auto size = duckdb_get_map_size(nullptr);
	REQUIRE(size == 0);
	size = duckdb_get_map_size(uint_val);
	REQUIRE(size == 0);

	auto key = duckdb_get_map_key(nullptr, 0);
	REQUIRE(!key);
	key = duckdb_get_map_key(uint_val, 0);
	REQUIRE(!key);

	auto value = duckdb_get_map_value(nullptr, 0);
	REQUIRE(!value);
	value = duckdb_get_map_value(uint_val, 0);
	REQUIRE(!value);

	duckdb_destroy_value(&uint_val);
}

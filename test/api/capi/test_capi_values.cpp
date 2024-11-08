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

TEST_CASE("Test LIST getters", "[capi]") {
	duckdb_value list_vals[2];
	list_vals[0] = duckdb_create_uint64(42);
	list_vals[1] = duckdb_create_uint64(43);
	duckdb_logical_type uint64_type = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
	duckdb_value list_value = duckdb_create_list_value(uint64_type, list_vals, 2);
	duckdb_destroy_value(&list_vals[0]);
	duckdb_destroy_value(&list_vals[1]);
	duckdb_destroy_logical_type(&uint64_type);

	auto size = duckdb_get_list_size(nullptr);
	REQUIRE(size == 0);

	size = duckdb_get_list_size(list_value);
	REQUIRE(size == 2);

	auto val = duckdb_get_list_child(nullptr, 0);
	REQUIRE(!val);
	duckdb_destroy_value(&val);

	val = duckdb_get_list_child(list_value, 0);
	REQUIRE(val);
	REQUIRE(duckdb_get_uint64(val) == 42);
	duckdb_destroy_value(&val);

	val = duckdb_get_list_child(list_value, 1);
	REQUIRE(val);
	REQUIRE(duckdb_get_uint64(val) == 43);
	duckdb_destroy_value(&val);

	val = duckdb_get_list_child(list_value, 2);
	REQUIRE(!val);
	duckdb_destroy_value(&val);

	duckdb_destroy_value(&list_value);
}

TEST_CASE("Test NULL value", "[capi]") {
	auto null_value = duckdb_create_null_value();
	REQUIRE(null_value);

	REQUIRE(!duckdb_is_null_value(nullptr));
	auto uint_val = duckdb_create_uint64(42);
	REQUIRE(!duckdb_is_null_value(uint_val));
	REQUIRE(duckdb_is_null_value(null_value));

	duckdb_destroy_value(&uint_val);
	duckdb_destroy_value(&null_value);
}

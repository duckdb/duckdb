#include "catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/common/insertion_order_preserving_map.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test Insertion Order Preserving Map: duplicate insert", "[api][.]") {
	InsertionOrderPreservingMap<int> map;

	map.insert("a", 1);
	map.insert("b", 2);
	map.insert("c", 3);
	map.insert("b", 4);

	auto it = map.find(string("c"));
	map.erase(it);

	int count = 0;
	for (auto it = map.begin(); it != map.end(); it++) {
		count++;
	}
	REQUIRE(count == 2);
}

TEST_CASE("Test Insertion Order Preserving Map: double erase", "[api][.]") {
	InsertionOrderPreservingMap<idx_t> map;
	map.insert("a", 1);
	map.insert("b", 2);
	map.insert("c", 3);
	map.insert("b", 4);
	map.erase(map.find("c"));

	int count = 0;
	for (auto it = map.begin(); it != map.end(); it++) {
		count++;
	}

	REQUIRE(count == 2);

	map.insert("a", 1);
	map.insert("b", 2);
	map.insert("c", 3);
	map.insert("b", 4);
	map.erase(map.find("c"));
	map.erase(map.find("b"));

	count = 0;
	for (auto it = map.begin(); it != map.end(); it++) {
		count++;
	}

	REQUIRE(count == 1);
	REQUIRE(map.find("b") == map.end());
}

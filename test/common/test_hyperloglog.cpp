#include "catch.hpp"
#include "duckdb/common/types/hyperloglog.hpp"

#include <vector>

using namespace duckdb;
using namespace std;

TEST_CASE("Test that hyperloglog works", "[hyperloglog]") {
	HyperLogLog log;
	// add a million elements of the same value
	int x = 4;
	for (size_t i = 0; i < 1000000; i++) {
		log.Add((uint8_t *)&x, sizeof(int));
	}
	REQUIRE(log.Count() == 1);

	// now add a million different values
	HyperLogLog log2;
	for (size_t i = 0; i < 1000000; i++) {
		x = i;
		log2.Add((uint8_t *)&x, sizeof(int));
	}
	// the count is approximate, but should be pretty close to a million
	size_t count = log2.Count();
	REQUIRE(count > 999000LL);
	REQUIRE(count < 1001000LL);

	// now we can merge the HLLs
	auto new_log = log.Merge(log2);
	// the count should be pretty much the same
	count = new_log->Count();
	REQUIRE(count > 999000LL);
	REQUIRE(count < 1001000LL);

	// now test composability of the merge
	// add everything to one big one
	// add chunks to small ones and then merge them
	// the result should be the same
	HyperLogLog big;
	HyperLogLog small[16];
	for (size_t i = 0; i < 1000000; i++) {
		x = ((2 * i) + 3) % (i + 3 / 2);
		big.Add((uint8_t *)&x, sizeof(int));
		small[i % 16].Add((uint8_t *)&x, sizeof(int));
	}
	// now merge them into one big HyperLogLog
	auto merged = HyperLogLog::Merge(small, 16);
	// the result should be identical to the big one
	REQUIRE(merged->Count() == big.Count());
}

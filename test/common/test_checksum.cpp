#include "catch.hpp"
#include "duckdb/common/checksum.hpp"

#include <vector>

using namespace duckdb;
using namespace std;

#define NUM_INTS 10

TEST_CASE("Checksum tests", "[checksum]") {
	// create a buffer
	int vals[NUM_INTS];
	for (size_t i = 0; i < NUM_INTS; i++) {
		vals[i] = i + 1;
	}
	// verify that checksum is consistent
	uint64_t c1 = Checksum((uint8_t *)vals, sizeof(int) * NUM_INTS);
	uint64_t c2 = Checksum((uint8_t *)vals, sizeof(int) * NUM_INTS);
	REQUIRE(c1 == c2);

	// verify that checksum is sort of good
	vals[3] = 1;
	uint64_t c3 = Checksum((uint8_t *)vals, sizeof(int) * NUM_INTS);
	REQUIRE(c1 != c3);

	// verify that zeros in the input does not zero the checksum
	vals[3] = 0;
	uint64_t c4 = Checksum((uint8_t *)vals, sizeof(int) * NUM_INTS);
	REQUIRE(c4 != 0);

	// zero at a different location should change the checksum
	vals[3] = 4;
	vals[4] = 0;
	uint64_t c5 = Checksum((uint8_t *)vals, sizeof(int) * NUM_INTS);
	REQUIRE(c4 != c5);

	REQUIRE(c1 != c4);
	REQUIRE(c1 != c5);
}

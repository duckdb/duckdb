#include "catch.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Numeric cast checks", "[numeric_cast]") {
#ifdef DUCKDB_CRASH_ON_ASSERT
	return;
#endif
	// unsigned-unsiged
	// can not fail upcasting unsigned type
	REQUIRE_NOTHROW(NumericCast<uint16_t, uint8_t>(NumericLimits<uint8_t>::Maximum()));
	REQUIRE_NOTHROW(NumericCast<uint16_t, uint8_t>(NumericLimits<uint8_t>::Minimum()));

	// we can down cast if value fits
	REQUIRE_NOTHROW(NumericCast<uint8_t, uint16_t>(NumericLimits<uint8_t>::Maximum()));

	// but not if it doesn't
	REQUIRE_THROWS(NumericCast<uint8_t, uint16_t>(NumericLimits<uint8_t>::Maximum() + 1));

	// signed-signed, same as above
	REQUIRE_NOTHROW(NumericCast<int16_t, int8_t>(NumericLimits<int8_t>::Maximum()));
	REQUIRE_NOTHROW(NumericCast<int16_t, int8_t>(NumericLimits<int8_t>::Minimum()));
	REQUIRE_NOTHROW(NumericCast<int8_t, int16_t>(NumericLimits<int8_t>::Maximum()));
	REQUIRE_THROWS(NumericCast<int8_t, int16_t>(NumericLimits<int8_t>::Maximum() + 1));
	REQUIRE_THROWS(NumericCast<int8_t, int16_t>(NumericLimits<int8_t>::Minimum() - 1));

	// unsigned to signed
	REQUIRE_NOTHROW(NumericCast<int8_t, uint8_t>(NumericLimits<int8_t>::Maximum()));
	REQUIRE_NOTHROW(NumericCast<int8_t, uint8_t>(NumericLimits<uint8_t>::Minimum()));

	// uint8 max will not fit in int8
	REQUIRE_THROWS(NumericCast<int8_t, uint8_t>(NumericLimits<uint8_t>::Maximum()));

	// signed to unsigned
	// can cast int8 max to uint8
	REQUIRE_NOTHROW(NumericCast<uint8_t, int8_t>(NumericLimits<int8_t>::Maximum()));
	// cat cast int8 min to unit8
	REQUIRE_THROWS(NumericCast<uint8_t, int8_t>(NumericLimits<int8_t>::Minimum()));

	// can't cast anything negative to anything unsigned
	REQUIRE_THROWS(NumericCast<uint64_t, int8_t>(-1));
	REQUIRE_THROWS(NumericCast<uint64_t, int16_t>(-1));
	REQUIRE_THROWS(NumericCast<uint64_t, int32_t>(-1));
	REQUIRE_THROWS(NumericCast<uint64_t, int64_t>(-1));

	// can't downcast big number
	REQUIRE_THROWS(NumericCast<int64_t, uint64_t>(NumericLimits<uint64_t>::Maximum()));
	REQUIRE_THROWS(NumericCast<int32_t, uint64_t>(NumericLimits<uint64_t>::Maximum()));
	REQUIRE_THROWS(NumericCast<uint32_t, uint64_t>(NumericLimits<uint64_t>::Maximum()));
	REQUIRE_THROWS(NumericCast<int16_t, uint64_t>(NumericLimits<uint64_t>::Maximum()));
	REQUIRE_THROWS(NumericCast<uint16_t, uint64_t>(NumericLimits<uint64_t>::Maximum()));
	REQUIRE_THROWS(NumericCast<int8_t, uint64_t>(NumericLimits<uint64_t>::Maximum()));
	REQUIRE_THROWS(NumericCast<uint8_t, uint64_t>(NumericLimits<uint64_t>::Maximum()));

	// TODO this should throw but doesn't
	//	REQUIRE_THROWS(NumericCast<uint8_t, hugeint_t>(hugeint_t(-1)));
}

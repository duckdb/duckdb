#include "catch.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/datetime.hpp"
#include "duckdb/common/helper.hpp"

#include <cstring>

using namespace duckdb;

static bool StrictParseTime(const char *str, idx_t len) {
	// copy into an exact-size allocation so any over-read is caught by the sanitizer
	auto buffer = make_unsafe_uniq_array<char>(len);
	memcpy(buffer.get(), str, len);
	dtime_t result;
	idx_t pos = 0;
	return Time::TryConvertTime(buffer.get(), len, pos, result, true);
}

TEST_CASE("Strict time parsing does not read past the end of the buffer", "[time]") {
	// single-digit minute at the end of the string: in strict mode the parser used to read the
	// seconds separator at buf[len]
	REQUIRE(!StrictParseTime("1:2", 3));
	REQUIRE(!StrictParseTime("123:4", 5));
	// trailing separator with nothing after it
	REQUIRE(!StrictParseTime("12:3", 4));

	// well-formed strict times still parse
	REQUIRE(StrictParseTime("11:22", 5));
	REQUIRE(StrictParseTime("11:22:33", 8));
}

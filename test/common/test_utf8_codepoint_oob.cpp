#include "catch.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/function/scalar/string_common.hpp"

#include <cstring>

using namespace duckdb;

// upper()/lower() decode each non-ascii character with Utf8Proc::UTF8ToCodepoint, which reads up
// to 4 bytes based on the leading byte. When a heap-backed (> 12 byte, not null-terminated) string
// ends in a truncated multi-byte sequence the decoder walks past the end of the buffer. Passing the
// remaining byte count lets it reject the truncated sequence instead of reading out of bounds.
TEST_CASE("lower on truncated trailing utf8 sequence stays in bounds", "[utf8]") {
	for (auto lead : {'\xc2', '\xe0', '\xf0'}) {
		auto buffer = make_unsafe_uniq_array<char>(20);
		memset(buffer.get(), 'A', 20);
		buffer[19] = lead; // multi-byte lead byte with no continuation bytes left in the buffer
		REQUIRE_THROWS(LowerLength(buffer.get(), 20));
	}
}

TEST_CASE("lower on complete trailing utf8 sequence is unchanged", "[utf8]") {
	// 18 ascii bytes followed by a full 2-byte 'Ä' (U+00C4) at the end of a heap-backed string
	auto buffer = make_unsafe_uniq_array<char>(20);
	memset(buffer.get(), 'A', 20);
	buffer[18] = '\xc3';
	buffer[19] = '\x84';
	REQUIRE(LowerLength(buffer.get(), 20) == 20);
}

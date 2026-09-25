#include "catch.hpp"
#include "duckdb/common/helper.hpp"
#include "utf8proc_wrapper.hpp"

#include <cstring>

using namespace duckdb;

// utf8proc_iterate returns a negative error code on invalid or truncated UTF-8. Callers that add
// the raw return value to a byte index (rtrim/ltrim/trim, lpad/rpad, strpos and the CSV header
// trimmer) underflow the index and read out of bounds. Utf8Proc::DecodeCharacter guards the return
// value so that iterating over invalid input always advances by at least one byte and stays in bounds.
TEST_CASE("Utf8Proc::DecodeCharacter guards invalid utf8", "[utf8]") {
	int32_t codepoint;

	// truncated multi-byte lead bytes with no continuation bytes: consume a single byte
	for (auto lead : {"\xc2", "\xe0", "\xf0"}) {
		codepoint = 0;
		REQUIRE(Utf8Proc::DecodeCharacter(lead, 1, codepoint) == 1);
		REQUIRE(codepoint == -1);
	}

	// a lone continuation byte is also invalid
	codepoint = 0;
	REQUIRE(Utf8Proc::DecodeCharacter("\x80", 1, codepoint) == 1);
	REQUIRE(codepoint == -1);

	// valid ascii and multi-byte characters are decoded as before
	codepoint = 0;
	REQUIRE(Utf8Proc::DecodeCharacter("A", 1, codepoint) == 1);
	REQUIRE(codepoint == 'A');
	codepoint = 0;
	REQUIRE(Utf8Proc::DecodeCharacter("\xc3\x84", 2, codepoint) == 2); // U+00C4
	REQUIRE(codepoint == 0xC4);
}

// walking a heap-backed buffer that ends in a truncated multi-byte sequence must stay within the
// buffer and terminate; before the fix the negative return underflowed the byte index
TEST_CASE("Iterating truncated trailing utf8 stays in bounds", "[utf8]") {
	for (auto lead : {'\xc2', '\xe0', '\xf0'}) {
		auto buffer = make_unsafe_uniq_array<char>(20);
		memset(buffer.get(), 'A', 20);
		buffer[19] = lead; // multi-byte lead byte with no continuation bytes left in the buffer
		idx_t pos = 0;
		idx_t chars = 0;
		while (pos < 20) {
			int32_t codepoint;
			pos += Utf8Proc::DecodeCharacter(buffer.get() + pos, 20 - pos, codepoint);
			chars++;
		}
		REQUIRE(pos == 20);
		REQUIRE(chars == 20);
	}
}

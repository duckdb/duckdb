#include "catch.hpp"
#include "duckdb/common/helper.hpp"
#include "utf8proc_wrapper.hpp"

#include <cstring>

using namespace duckdb;

// Utf8Proc::UTF8ToCodepoint reads the continuation bytes implied by the lead
// byte. When a heap-backed (> 12 byte, not null-terminated) string ends in a
// truncated multi-byte sequence the decoder used to read past the end of the
// buffer. This path is reached by upper/lower/translate and by the grapheme
// iterator (reverse, length, ...) on non-utf8 input, which can be produced
// through the C API / appender as those do not validate UTF-8.
TEST_CASE("UTF8ToCodepoint stays in bounds on a truncated trailing sequence", "[utf8]") {
	auto buffer = make_unsafe_uniq_array<char>(13);
	memset(buffer.get(), 'a', 13);

	// 2-, 3- and 4-byte lead bytes with no continuation bytes following
	for (auto lead : {'\xc2', '\xe0', '\xf0'}) {
		buffer.get()[12] = lead;

		int sz = 0;
		// only one byte is available, the decoder must not read the missing bytes
		REQUIRE_THROWS(Utf8Proc::UTF8ToCodepoint(buffer.get() + 12, sz, 1));

		// grapheme iteration (reverse, length, ...) decodes the final byte
		REQUIRE_THROWS(Utf8Proc::GraphemeCount(buffer.get(), 13));
	}
}

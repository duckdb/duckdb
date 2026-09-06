#include "catch.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/function/scalar/string_common.hpp"

#include <cstring>

using namespace duckdb;

// substring() scans byte-by-byte to find unicode character boundaries. On a
// negative offset the backward scan can leave start_pos at 0 when the input has
// no character boundary; the loop that skips continuation bytes then walks past
// the end of the buffer. A heap-backed (> 12 byte) string of continuation bytes
// reproduces the out-of-bounds read.
TEST_CASE("substring negative offset on non-utf8 input stays in bounds", "[substring]") {
	Vector result(LogicalType::VARCHAR);

	auto buffer = make_unsafe_uniq_array<char>(20);
	memset(buffer.get(), '\x80', 20);
	string_t input(buffer.get(), 20);

	auto out = SubstringUnicode(result, input, -1, 5);
	REQUIRE(out.GetSize() == 0);
}

#include "catch.hpp"
#include "duckdb.hpp"
#include "test_helpers.hpp"

using namespace duckdb;

// The query appender runs its query over the appended strings without validating them as UTF-8, so encode can see a
// VARCHAR ending in a truncated multi-byte sequence. It must not read the missing continuation bytes past the end of
// the (heap-backed, > 12 byte) string.
TEST_CASE("encode rejects a truncated UTF-8 sequence at the end of the input", "[encode]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t (b BLOB)"));
	for (auto &encoding : {"latin-1", "utf-16"}) {
		for (auto value : {"abcdefghijklm\xC3", "abcdefghijklm\xE6\x97", "abcdefghijklm\xF0\x9F\x98"}) {
			auto query = StringUtil::Format("INSERT INTO t SELECT encode(col1, '%s') FROM appended_data", encoding);
			QueryAppender appender(con, query, {LogicalType::VARCHAR});
			appender.BeginRow();
			appender.Append(string_t(value, UnsafeNumericCast<uint32_t>(strlen(value))));
			appender.EndRow();
			REQUIRE_THROWS_WITH(appender.Close(), Catch::Contains("Could not encode the character at byte 13"));
		}
	}
	auto result = con.Query("SELECT count(*) FROM t");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
}

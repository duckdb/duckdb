#include "catch.hpp"
#include "duckdb/main/appender.hpp"
#include "test_helpers.hpp"

using namespace duckdb;

// Normalizer::Decode (extension/icu/collation) decodes a 4-byte UTF-8 sequence without rejecting
// code points past the Unicode range. A sequence such as F7 BF BF BF decodes to U+1FFFFF, which is
// then used to index the fixed-size collation tables: TailoredBlocks::Contains reads the tailoring
// bitmap at (cp >> 6) / 64 and LookupValue reads trie_stage1[cp >> 6], both out of bounds. Invalid
// UTF-8 reaches the collator through the appender, which does not validate UTF-8.
TEST_CASE("icu collation on an out-of-range code point stays in bounds", "[icu]") {
	duckdb::unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	if (!db.ExtensionIsLoaded("icu")) {
		return;
	}

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t(s VARCHAR)"));
	{
		Appender appender(con, "t");
		// a valid non-ascii value first, so the segment statistics accept the following invalid one
		const char valid[2] = {(char)0xC3, (char)0xA4}; // U+00E4
		appender.BeginRow();
		appender.Append(string_t(valid, 2));
		appender.EndRow();
		// F7 BF BF BF decodes to U+1FFFFF, past U+10FFFF
		const char invalid[4] = {(char)0xF7, (char)0xBF, (char)0xBF, (char)0xBF};
		appender.BeginRow();
		appender.Append(string_t(invalid, 4));
		appender.EndRow();
		appender.Close();
	}

	// the out-of-range code point must collate as U+FFFD without reading out of bounds
	result = con.Query("SELECT count(*) FROM t WHERE icu_sort_key(s, 'en') = icu_sort_key(chr(65533), 'en')");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
}

#include "catch.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/parser/peg/matcher.hpp"

using namespace duckdb;

TEST_CASE("Parser keyword registration validates keyword spelling", "[api][parser]") {
	ParserCache cache;

	REQUIRE_NOTHROW(cache.RegisterKeyword("ab", ExtensionKeywordCategory::RESERVED));
	REQUIRE_NOTHROW(cache.RegisterKeyword("__", ExtensionKeywordCategory::RESERVED));
	REQUIRE_NOTHROW(cache.RegisterKeyword("valid$keyword", ExtensionKeywordCategory::UNRESERVED));

	for (const auto &keyword : {"", "a", "_", "1", "12", "12.5", "1keyword", "key1word", "keyword1"}) {
		REQUIRE_THROWS_AS(cache.RegisterKeyword(keyword, ExtensionKeywordCategory::RESERVED), InvalidInputException);
	}
}

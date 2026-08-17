#include "catch.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/main/extension_callback_manager.hpp"

using namespace duckdb;

TEST_CASE("Parser keyword registration validates keyword spelling", "[api][parser]") {
	ExtensionCallbackManager manager;

	for (const auto &keyword : {ExtensionKeyword {"ab", ExtensionKeywordCategory::RESERVED},
	                            ExtensionKeyword {"__", ExtensionKeywordCategory::RESERVED},
	                            ExtensionKeyword {"valid$keyword", ExtensionKeywordCategory::UNRESERVED}}) {
		REQUIRE_NOTHROW(manager.Register(vector<ExtensionKeyword> {keyword}));
	}

	for (const auto &keyword : {"", "a", "_", "1", "12", "12.5", "1keyword", "key1word", "keyword1", "$keyword",
	                            "key-word", "two words", "schema.word", "\"quoted\"", "keyword/operator"}) {
		REQUIRE_THROWS_AS(manager.Register(vector<ExtensionKeyword> {{keyword, ExtensionKeywordCategory::RESERVED}}),
		                  InvalidInputException);
	}
}

TEST_CASE("Parser keyword batches are registered atomically", "[api][parser]") {
	ExtensionCallbackManager manager;
	vector<ExtensionKeyword> keywords {{"batch_keyword", ExtensionKeywordCategory::RESERVED},
	                                   {"batch_keyword", ExtensionKeywordCategory::UNRESERVED}};

	REQUIRE_THROWS_AS(manager.Register(keywords), InvalidInputException);
	REQUIRE_FALSE(manager.GetKeywordExtension()->IsKeyword("batch_keyword"));
	REQUIRE_FALSE(manager.HasParserExtensions());
}

TEST_CASE("Promoted built-in parser keywords do not duplicate metadata", "[api][parser]") {
	ExtensionCallbackManager manager;
	manager.Register(vector<ExtensionKeyword> {{"generated", ExtensionKeywordCategory::TYPE_NAME}});

	idx_t generated_count = 0;
	for (const auto &keyword : manager.GetKeywordExtension()->KeywordList()) {
		if (keyword.name == "generated" && keyword.category == KeywordCategory::KEYWORD_TYPE_FUNC) {
			generated_count++;
		}
	}
	REQUIRE(generated_count == 1);
}

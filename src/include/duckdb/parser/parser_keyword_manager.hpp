//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parser_keyword_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/mutex.hpp"

namespace duckdb {

enum class KeywordCategory : uint8_t {
	KEYWORD_RESERVED,
	KEYWORD_UNRESERVED,
	KEYWORD_TYPE_FUNC,
	KEYWORD_COL_NAME,
	KEYWORD_NONE
};

struct ParserKeyword {
	string name;
	KeywordCategory category;
};

class ParserKeywordManager {
public:
	//! Register a new keyword for a specific category. Thread-safe.
	void RegisterKeyword(const string &keyword, KeywordCategory category);

	//! Checks if the given text is a registered keyword of any category.
	bool IsKeyword(const string &text) const;

	//! Checks if a keyword belongs to a specific category.
	bool IsKeywordInCategory(const string &text, KeywordCategory category) const;

private:
	//! Separate sets for each keyword category.
	case_insensitive_set_t reserved_keywords;
	case_insensitive_set_t unreserved_keywords;
	case_insensitive_set_t type_func_keywords;
	case_insensitive_set_t col_name_keywords;

	//! Mutex to protect the sets during concurrent registrations.
	mutex keyword_mutex;
};
} // namespace duckdb

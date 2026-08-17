//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/keyword_extension.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/parser/simplified_token.hpp"

namespace duckdb {

class ExtensionCallbackManager;
enum class PEGKeywordCategory : uint8_t;

//! Immutable lookup state for keywords registered by extensions.
class KeywordExtension {
public:
	bool IsKeyword(const string &text) const;
	bool KeywordCategoryType(const string &text, PEGKeywordCategory category) const;
	vector<ParserKeyword> KeywordList() const;

private:
	friend class ExtensionCallbackManager;

	void RegisterKeyword(const ExtensionKeyword &keyword);
	PEGKeywordCategory LookupCategory(const string &text) const;
	bool HasCategory(const string &text, PEGKeywordCategory category) const;

private:
	case_insensitive_set_t reserved;
	case_insensitive_set_t unreserved;
	case_insensitive_set_t column_name;
	case_insensitive_set_t function_name;
	case_insensitive_set_t type_name;
};

} // namespace duckdb

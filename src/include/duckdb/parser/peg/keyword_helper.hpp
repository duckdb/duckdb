//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/peg/keyword_helper.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/parser/peg/grammar_literal_table.hpp"
#include "duckdb/parser/simplified_token.hpp"
#include "duckdb/parser/peg/literal_info.hpp"

namespace duckdb {

class GrammarLiteralTable;

enum class SuggestionState : uint8_t;

class PEGKeywordHelper {
public:
	virtual ~PEGKeywordHelper() = default;

public:
	LiteralInfo LookupKeyword(const string &text) const {
		return GetLiteralTable().Lookup(text);
	}
	bool IsKeyword(const string &text) const {
		return LookupKeyword(text).IsKeyword();
	}
	//! Opaque flags accepted in this identifier position, computed when creating a matcher.
	virtual keyword_categories_t GetIdentifierMask(SuggestionState type) const = 0;
	virtual vector<ParserKeyword> KeywordList() const = 0;
	//! Every helper provides an immutable table containing its literals and keyword flags.
	virtual const GrammarLiteralTable &GetLiteralTable() const = 0;
};

} // namespace duckdb

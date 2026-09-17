//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/peg/keyword_helper.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/parser/simplified_token.hpp"

namespace duckdb {

class GrammarLiteralTable;

enum class PEGKeywordCategory : uint8_t {
	KEYWORD_NONE,
	KEYWORD_UNRESERVED,
	KEYWORD_RESERVED,
	KEYWORD_TYPE_FUNC,
	KEYWORD_COL_NAME,
	KEYWORD_TYPE_NAME
};

class PEGKeywordHelper {
public:
	virtual ~PEGKeywordHelper() = default;

public:
	virtual bool KeywordCategoryType(const string &text, PEGKeywordCategory type) const = 0;
	virtual bool IsKeyword(const string &text) const = 0;
	virtual vector<ParserKeyword> KeywordList() const = 0;
	//! Opt in only when this immutable table agrees with the helper's keyword predicates.
	virtual optional_ptr<const GrammarLiteralTable> GetLiteralTable() const {
		return nullptr;
	}
};

} // namespace duckdb

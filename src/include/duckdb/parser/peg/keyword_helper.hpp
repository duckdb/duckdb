//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/peg/keyword_helper.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/parser/simplified_token.hpp"

namespace duckdb {

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
};

} // namespace duckdb

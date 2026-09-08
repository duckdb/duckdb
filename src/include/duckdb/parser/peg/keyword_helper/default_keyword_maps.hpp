#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/parser/peg/keyword_helper.hpp"
#include "duckdb/parser/simplified_token.hpp"

namespace duckdb {

class DefaultKeywordMaps {
public:
	bool IsKeywordOfCategory(const string &text, PEGKeywordCategory type) const;
	bool IsKeyword(const string &text) const;
	vector<ParserKeyword> ToList() const;

public:
	case_insensitive_set_t reserved_keyword_map;
	case_insensitive_set_t unreserved_keyword_map;
	case_insensitive_set_t colname_keyword_map;
	case_insensitive_set_t typefunc_keyword_map;
	case_insensitive_set_t typename_keyword_map;
};

} // namespace duckdb

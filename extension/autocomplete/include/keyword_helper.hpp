#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/parser/simplified_token.hpp"

namespace duckdb {
class PEGKeywordHelper {
public:
	static PEGKeywordHelper &Instance();
	bool KeywordCategoryType(const string &text, KeywordCategory type) const;
	void InitializeKeywordMaps();

private:
	PEGKeywordHelper();
	bool initialized;
	case_insensitive_set_t reserved_keyword_map;
	case_insensitive_set_t unreserved_keyword_map;
	case_insensitive_set_t colname_keyword_map;
	case_insensitive_set_t typefunc_keyword_map;
};
} // namespace duckdb

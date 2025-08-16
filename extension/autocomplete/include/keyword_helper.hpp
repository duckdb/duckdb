#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/string.hpp"

namespace duckdb {
enum class KeywordCategory : uint8_t {
	KEYWORD_NONE,
	KEYWORD_UNRESERVED,
	KEYWORD_RESERVED,
	KEYWORD_TYPE_FUNC,
	KEYWORD_COL_NAME
};

class KeywordHelper {
public:
	static KeywordHelper &Instance();
	bool KeywordCategoryType(const string &text, KeywordCategory type) const;
	void InitializeKeywordMaps();

private:
	KeywordHelper();
	bool initialized;
	case_insensitive_set_t reserved_keyword_map;
	case_insensitive_set_t unreserved_keyword_map;
	case_insensitive_set_t colname_keyword_map;
	case_insensitive_set_t typefunc_keyword_map;
};
} // namespace duckdb

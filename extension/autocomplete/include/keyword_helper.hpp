#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/string.hpp"

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

class KeywordHelper {
public:
	static KeywordHelper &Instance();
	bool IsKeyword(const string &text) const;
	KeywordCategory KeywordCategoryType(const string &text) const;
	void InitializeKeywordMap();
	void InsertKeyword(const string &kw, KeywordCategory cat);

private:
	KeywordHelper();
	bool initialized;
	case_insensitive_map_t<KeywordCategory> keyword_map;
};
} // namespace duckdb

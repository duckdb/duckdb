#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/string.hpp"

namespace duckdb {
enum class KeywordCategory : uint8_t {
	KEYWORD_NONE = 0,            // 00000000
	KEYWORD_UNRESERVED = 1 << 0, // 00000001 (1)
	KEYWORD_RESERVED = 1 << 1,   // 00000010 (2)
	KEYWORD_TYPE_FUNC = 1 << 2,  // 00000100 (4)
	KEYWORD_COL_NAME = 1 << 3    // 00001000 (8)
};

inline KeywordCategory operator|(KeywordCategory a, KeywordCategory b) {
	return static_cast<KeywordCategory>(static_cast<uint8_t>(a) | static_cast<uint8_t>(b));
}
inline KeywordCategory operator&(KeywordCategory a, KeywordCategory b) {
	return static_cast<KeywordCategory>(static_cast<uint8_t>(a) & static_cast<uint8_t>(b));
}
inline KeywordCategory &operator|=(KeywordCategory &a, KeywordCategory b) {
	a = a | b;
	return a;
}

inline KeywordCategory operator~(KeywordCategory a) {
	return static_cast<KeywordCategory>(~static_cast<uint8_t>(a));
}

struct ParserKeyword {
	string name;
	KeywordCategory category;
};

class KeywordHelper {
public:
	static KeywordHelper &Instance();
	KeywordCategory KeywordCategoryType(const string &text) const;
	void InitializeKeywordMap();

private:
	KeywordHelper();
	bool initialized;
	case_insensitive_map_t<KeywordCategory> keyword_map;
};
} // namespace duckdb

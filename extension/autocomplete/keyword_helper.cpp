#include "keyword_helper.hpp"

namespace duckdb {
KeywordHelper &KeywordHelper::Instance() {
	static KeywordHelper instance;
	return instance;
}

KeywordHelper::KeywordHelper() {
	InitializeKeywordMap();
}

bool KeywordHelper::IsKeyword(const std::string &text) const {
	return keyword_map.find(text) != keyword_map.end();
}

KeywordCategory KeywordHelper::KeywordCategoryType(const std::string &text) const {
	auto it = keyword_map.find(text);
	if (it != keyword_map.end()) {
		return it->second;
	}
	return KeywordCategory::KEYWORD_NONE;
}
} // namespace duckdb

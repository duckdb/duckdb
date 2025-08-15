#include "keyword_helper.hpp"

namespace duckdb {
KeywordHelper &KeywordHelper::Instance() {
	static KeywordHelper instance;
	return instance;
}

KeywordHelper::KeywordHelper() {
	InitializeKeywordMaps();
}

bool KeywordHelper::KeywordCategoryType(const std::string &text, const KeywordCategory type) const {
	switch (type) {
	case KeywordCategory::KEYWORD_RESERVED: {
		auto it = reserved_keyword_map.find(text);
		return it != reserved_keyword_map.end();
	}
	case KeywordCategory::KEYWORD_UNRESERVED: {
		auto it = unreserved_keyword_map.find(text);
		return it != unreserved_keyword_map.end();
	}
	case KeywordCategory::KEYWORD_TYPE_FUNC: {
		auto it = typefunc_keyword_map.find(text);
		return it != typefunc_keyword_map.end();
	}
	case KeywordCategory::KEYWORD_COL_NAME: {
		auto it = colname_keyword_map.find(text);
		return it != colname_keyword_map.end();
	}
	default:
		return false;
	}
}
} // namespace duckdb

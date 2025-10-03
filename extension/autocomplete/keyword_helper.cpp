#include "keyword_helper.hpp"

namespace duckdb {
PEGKeywordHelper &PEGKeywordHelper::Instance() {
	static PEGKeywordHelper instance;
	return instance;
}

PEGKeywordHelper::PEGKeywordHelper() {
	InitializeKeywordMaps();
}

bool PEGKeywordHelper::KeywordCategoryType(const std::string &text, const PEGKeywordCategory type) const {
	switch (type) {
	case PEGKeywordCategory::KEYWORD_RESERVED: {
		auto it = reserved_keyword_map.find(text);
		return it != reserved_keyword_map.end();
	}
	case PEGKeywordCategory::KEYWORD_UNRESERVED: {
		auto it = unreserved_keyword_map.find(text);
		return it != unreserved_keyword_map.end();
	}
	case PEGKeywordCategory::KEYWORD_TYPE_FUNC: {
		auto it = typefunc_keyword_map.find(text);
		return it != typefunc_keyword_map.end();
	}
	case PEGKeywordCategory::KEYWORD_COL_NAME: {
		auto it = colname_keyword_map.find(text);
		return it != colname_keyword_map.end();
	}
	default:
		return false;
	}
}
} // namespace duckdb

#include "keyword_helper.hpp"
#include "duckdb/common/string_util.hpp"

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

vector<string> PEGKeywordHelper::FindReservedKeywordsByPrefix(const string &prefix) const {
	vector<string> matches;
	auto prefix_upper = StringUtil::Upper(prefix);
	for (auto &keyword : reserved_keyword_map) {
		auto keyword_upper = StringUtil::Upper(keyword);
		if (keyword_upper.size() > prefix_upper.size() &&
		    keyword_upper.compare(0, prefix_upper.size(), prefix_upper) == 0) {
			matches.push_back(keyword_upper);
		}
	}
	return matches;
}
} // namespace duckdb

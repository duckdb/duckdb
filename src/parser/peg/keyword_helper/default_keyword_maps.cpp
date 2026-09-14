#include "duckdb/parser/peg/keyword_helper/default_keyword_maps.hpp"

namespace duckdb {

bool DefaultKeywordMaps::IsKeywordOfCategory(const string &text, PEGKeywordCategory category) const {
	switch (category) {
	case PEGKeywordCategory::KEYWORD_RESERVED:
		return reserved_keyword_map.count(text) != 0;
	case PEGKeywordCategory::KEYWORD_UNRESERVED:
		return unreserved_keyword_map.count(text) != 0;
	case PEGKeywordCategory::KEYWORD_TYPE_FUNC:
		return typefunc_keyword_map.count(text) != 0;
	case PEGKeywordCategory::KEYWORD_COL_NAME:
		return colname_keyword_map.count(text) != 0;
	case PEGKeywordCategory::KEYWORD_TYPE_NAME:
		return typename_keyword_map.count(text) != 0;
	default:
		return false;
	}
}

bool DefaultKeywordMaps::IsKeyword(const string &text) const {
	return reserved_keyword_map.count(text) != 0 || unreserved_keyword_map.count(text) != 0 ||
	       colname_keyword_map.count(text) != 0 || typefunc_keyword_map.count(text) != 0 ||
	       typename_keyword_map.count(text) != 0;
}

vector<ParserKeyword> DefaultKeywordMaps::ToList() const {
	vector<ParserKeyword> result;
	for (auto &kw : reserved_keyword_map) {
		result.push_back({kw, KeywordCategory::KEYWORD_RESERVED});
	}
	for (auto &kw : unreserved_keyword_map) {
		result.push_back({kw, KeywordCategory::KEYWORD_UNRESERVED});
	}
	for (auto &kw : typefunc_keyword_map) {
		result.push_back({kw, KeywordCategory::KEYWORD_TYPE_FUNC});
	}
	for (auto &kw : colname_keyword_map) {
		result.push_back({kw, KeywordCategory::KEYWORD_COL_NAME});
	}
	return result;
}

} // namespace duckdb

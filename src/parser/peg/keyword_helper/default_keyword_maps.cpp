#include "duckdb/parser/peg/keyword_helper/default_keyword_maps.hpp"

namespace duckdb {

void DefaultKeywordMaps::AddCategory(const case_insensitive_set_t &keywords, PEGKeywordCategory category) {
	for (auto &keyword : keywords) {
		keyword_categories[keyword] |= PEGKeywordHelper::CategoryBit(category);
	}
}

void DefaultKeywordMaps::Finalize() {
	AddCategory(reserved_keyword_map, PEGKeywordCategory::KEYWORD_RESERVED);
	AddCategory(unreserved_keyword_map, PEGKeywordCategory::KEYWORD_UNRESERVED);
	AddCategory(typefunc_keyword_map, PEGKeywordCategory::KEYWORD_TYPE_FUNC);
	AddCategory(colname_keyword_map, PEGKeywordCategory::KEYWORD_COL_NAME);
	AddCategory(typename_keyword_map, PEGKeywordCategory::KEYWORD_TYPE_NAME);
}

bool DefaultKeywordMaps::IsKeywordOfCategory(const string &text, PEGKeywordCategory category) const {
	return (KeywordCategories(text) & PEGKeywordHelper::CategoryBit(category)) != 0;
}

bool DefaultKeywordMaps::IsKeyword(const string &text) const {
	return keyword_categories.find(text) != keyword_categories.end();
}

uint8_t DefaultKeywordMaps::KeywordCategories(const string &text) const {
	auto entry = keyword_categories.find(text);
	if (entry == keyword_categories.end()) {
		return 0;
	}
	return entry->second;
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

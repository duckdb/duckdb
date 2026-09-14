#include "duckdb/parser/peg/keyword_helper/duckdb_keyword_helper.hpp"

namespace duckdb {

DuckDBKeywordHelper::DuckDBKeywordHelper() : initialized(false) {
	InitializeKeywordMaps();
}

const DuckDBKeywordHelper &DuckDBKeywordHelper::Instance() {
	static DuckDBKeywordHelper instance;
	return instance;
}

bool DuckDBKeywordHelper::KeywordCategoryType(const std::string &text, const PEGKeywordCategory category) const {
	return keyword_maps.IsKeywordOfCategory(text, category);
}

bool DuckDBKeywordHelper::IsKeyword(const string &text) const {
	return keyword_maps.IsKeyword(text);
};

vector<ParserKeyword> DuckDBKeywordHelper::KeywordList() const {
	return keyword_maps.ToList();
}

} // namespace duckdb

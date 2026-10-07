#include "duckdb/parser/peg/keyword_helper/duckdb_keyword_helper.hpp"
#include "duckdb/parser/peg/parsed_grammar.hpp"

namespace duckdb {

DuckDBKeywordHelper::DuckDBKeywordHelper() : keyword_maps(InitializeKeywordMaps()) {
}

const GrammarLiteralTable &DuckDBKeywordHelper::GetLiteralTable() const {
	lock_guard<mutex> guard(literal_table_lock);
	if (!literal_table) {
		literal_table = make_uniq<GrammarLiteralTable>(ParsedGrammar::ParseDefault(), keyword_maps.ToLiteralMap());
	}
	return *literal_table;
}

const DuckDBKeywordHelper &DuckDBKeywordHelper::Instance() {
	static DuckDBKeywordHelper instance;
	return instance;
}

keyword_categories_t DuckDBKeywordHelper::GetIdentifierMask(SuggestionState type) const {
	return DefaultKeywordMaps::GetIdentifierMask(type);
}

KeywordCategory DuckDBKeywordHelper::GetKeywordCategory(const string &text) const {
	return DefaultKeywordMaps::GetKeywordCategory(LookupKeyword(text));
}

vector<ParserKeyword> DuckDBKeywordHelper::KeywordList() const {
	return keyword_maps.ToList();
}

} // namespace duckdb

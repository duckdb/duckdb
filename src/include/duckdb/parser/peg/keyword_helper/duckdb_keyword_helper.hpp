#pragma once

#include "duckdb/parser/peg/keyword_helper.hpp"
#include "duckdb/parser/peg/keyword_helper/default_keyword_maps.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/unique_ptr.hpp"

namespace duckdb {

class DuckDBKeywordHelper : public PEGKeywordHelper {
private:
	DuckDBKeywordHelper();

public:
	static const DuckDBKeywordHelper &Instance();

public:
	keyword_categories_t GetIdentifierMask(SuggestionState type) const override;
	KeywordCategory GetKeywordCategory(const string &text) const;
	vector<ParserKeyword> KeywordList() const override;
	const GrammarLiteralTable &GetLiteralTable() const override;

private:
	static DefaultKeywordMaps InitializeKeywordMaps();

private:
	DefaultKeywordMaps keyword_maps;
	// built on first use: it needs the full grammar, which plain keyword lookups do not
	mutable mutex literal_table_lock;
	mutable unique_ptr<GrammarLiteralTable> literal_table;
};

} // namespace duckdb

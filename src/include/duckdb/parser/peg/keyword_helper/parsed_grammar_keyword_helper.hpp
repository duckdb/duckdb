#pragma once

#include "duckdb/parser/peg/keyword_helper.hpp"
#include "duckdb/parser/peg/keyword_helper/default_keyword_maps.hpp"
#include "duckdb/parser/peg/grammar_literal_table.hpp"

namespace duckdb {

class ParsedGrammar;

class ParsedGrammarKeywordHelper : public PEGKeywordHelper {
public:
	explicit ParsedGrammarKeywordHelper(const ParsedGrammar &grammar);

public:
	bool KeywordCategoryType(const string &text, PEGKeywordCategory type) const override;
	bool IsKeyword(const string &text) const override;
	vector<ParserKeyword> KeywordList() const override;
	optional_ptr<const GrammarLiteralTable> GetLiteralTable() const override {
		return literal_table;
	}

private:
	DefaultKeywordMaps keyword_maps;
	GrammarLiteralTable literal_table;
};

} // namespace duckdb

#pragma once

#include "duckdb/parser/peg/keyword_helper.hpp"

namespace duckdb {

class ParsedGrammar;

class ParsedGrammarKeywordHelper : public PEGKeywordHelper {
public:
	explicit ParsedGrammarKeywordHelper(const ParsedGrammar &grammar);

public:
	bool KeywordCategoryType(const string &text, PEGKeywordCategory type) const override;
	bool IsKeyword(const string &text) const override;
	vector<ParserKeyword> KeywordList() const override;

private:
	case_insensitive_set_t reserved_keyword_map;
	case_insensitive_set_t unreserved_keyword_map;
	case_insensitive_set_t colname_keyword_map;
	case_insensitive_set_t typefunc_keyword_map;
	case_insensitive_set_t typename_keyword_map;
};

} // namespace duckdb

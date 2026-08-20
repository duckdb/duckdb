#pragma once

#include "duckdb/parser/peg/keyword_helper.hpp"

namespace duckdb {

class DuckDBKeywordHelper : public PEGKeywordHelper {
private:
	DuckDBKeywordHelper();

public:
	static const DuckDBKeywordHelper &Instance();

public:
	bool KeywordCategoryType(const std::string &text, const PEGKeywordCategory type) const override;
	bool IsKeyword(const string &text) const override;
	vector<ParserKeyword> KeywordList() const override;

private:
	void InitializeKeywordMaps();

private:
	case_insensitive_set_t reserved_keyword_map;
	case_insensitive_set_t unreserved_keyword_map;
	case_insensitive_set_t colname_keyword_map;
	case_insensitive_set_t typefunc_keyword_map;
	case_insensitive_set_t typename_keyword_map;

private:
	bool initialized;
};

} // namespace duckdb

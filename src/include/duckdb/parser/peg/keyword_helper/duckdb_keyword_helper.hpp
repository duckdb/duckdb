#pragma once

#include "duckdb/parser/peg/keyword_helper.hpp"
#include "duckdb/parser/peg/keyword_helper/default_keyword_maps.hpp"

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
	DefaultKeywordMaps keyword_maps;

private:
	bool initialized;
};

} // namespace duckdb

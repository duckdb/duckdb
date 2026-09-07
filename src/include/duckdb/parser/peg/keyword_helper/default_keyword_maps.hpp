#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/parser/peg/keyword_helper.hpp"
#include "duckdb/parser/simplified_token.hpp"

namespace duckdb {

class DefaultKeywordMaps {
public:
	bool IsKeywordOfCategory(const string &text, PEGKeywordCategory type) const;
	bool IsKeyword(const string &text) const;
	//! Bit set (see PEGKeywordHelper::CategoryBit) of the categories the text belongs to, 0 if it is not a keyword
	uint8_t KeywordCategories(const string &text) const;
	vector<ParserKeyword> ToList() const;
	//! Builds the combined lookup map from the category sets; must be called once the sets are populated
	void Finalize();

public:
	case_insensitive_set_t reserved_keyword_map;
	case_insensitive_set_t unreserved_keyword_map;
	case_insensitive_set_t colname_keyword_map;
	case_insensitive_set_t typefunc_keyword_map;
	case_insensitive_set_t typename_keyword_map;

private:
	void AddCategory(const case_insensitive_set_t &keywords, PEGKeywordCategory category);

	//! Every keyword mapped to the bit set of categories it belongs to, so that a lookup hashes the text once
	//! instead of once per category set
	case_insensitive_map_t<uint8_t> keyword_categories;
};

} // namespace duckdb

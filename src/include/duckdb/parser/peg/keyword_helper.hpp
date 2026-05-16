#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/parser/simplified_token.hpp"

namespace duckdb {
class ClientContext;
class DatabaseInstance;

enum class PEGKeywordCategory : uint8_t {
	KEYWORD_NONE,
	KEYWORD_UNRESERVED,
	KEYWORD_RESERVED,
	KEYWORD_TYPE_FUNC,
	KEYWORD_COL_NAME,
	KEYWORD_TYPE_NAME
};

class PEGKeywordHelper {
public:
	PEGKeywordHelper();

	static shared_ptr<PEGKeywordHelper> Get(ClientContext &context);
	static shared_ptr<PEGKeywordHelper> Get(DatabaseInstance &db);
	bool KeywordCategoryType(const string &text, PEGKeywordCategory type) const;
	void InitializeKeywordMaps();
	bool IsKeyword(const string &text) {
		if (reserved_keyword_map.count(text) != 0 || unreserved_keyword_map.count(text) != 0 ||
		    colname_keyword_map.count(text) != 0 || typefunc_keyword_map.count(text) != 0) {
			return true;
		}
		return false;
	};
	vector<ParserKeyword> KeywordList();

private:
	bool initialized = false;
	case_insensitive_set_t reserved_keyword_map;
	case_insensitive_set_t unreserved_keyword_map;
	case_insensitive_set_t colname_keyword_map;
	case_insensitive_set_t typefunc_keyword_map;
	case_insensitive_set_t typename_keyword_map;
};
} // namespace duckdb

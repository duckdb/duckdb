#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

bool KeywordHelper::IsKeyword(const string &text) {
	return Parser::IsKeyword(text);
}

bool KeywordHelper::RequiresQuotes(const string &text) {
	for(size_t i = 0; i < text.size(); i++) {
		if (i > 0 && (text[i] >= '0' && text[i] <= '9')) {
			continue;
		}
		if (text[i] >= 'a' && text[i] <= 'z') {
			continue;
		}
		return true;
	}
	return IsKeyword(text);
}

string KeywordHelper::WriteOptionallyQuoted(const string &text) {
	if (!RequiresQuotes(text)) {
		return text;
	}
	return "\"" + StringUtil::Replace(text, "\"", "\"\"") + "\"";
}

}
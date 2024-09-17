#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

bool KeywordHelper::IsKeyword(const string &text) {
	return Parser::IsKeyword(text) != KeywordCategory::KEYWORD_NONE;
}

KeywordCategory KeywordHelper::KeywordCategoryType(const string &text) {
	return Parser::IsKeyword(text);
}

bool KeywordHelper::RequiresQuotes(const string &text, bool allow_caps) {
	for (size_t i = 0; i < text.size(); i++) {
		if (i > 0 && (text[i] >= '0' && text[i] <= '9')) {
			continue;
		}
		if (text[i] >= 'a' && text[i] <= 'z') {
			continue;
		}
		if (allow_caps) {
			if (text[i] >= 'A' && text[i] <= 'Z') {
				continue;
			}
		}
		if (text[i] == '_') {
			continue;
		}
		return true;
	}
	return IsKeyword(text);
}

string KeywordHelper::EscapeQuotes(const string &text, char quote) {
	return StringUtil::Replace(text, string(1, quote), string(2, quote));
}

string KeywordHelper::WriteQuoted(const string &text, char quote) {
	// 1. Escapes all occurences of 'quote' by doubling them (escape in SQL)
	// 2. Adds quotes around the string
	return string(1, quote) + EscapeQuotes(text, quote) + string(1, quote);
}

string KeywordHelper::WriteOptionallyQuoted(const string &text, char quote, bool allow_caps) {
	if (!RequiresQuotes(text, allow_caps)) {
		return text;
	}
	return WriteQuoted(text, quote);
}

} // namespace duckdb

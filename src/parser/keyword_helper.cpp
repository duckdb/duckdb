#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/parser/peg/keyword_helper.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

static KeywordCategory GetPEGKeywordCategory(const string &text) {
	auto &helper = PEGKeywordHelper::Instance();
	if (helper.KeywordCategoryType(text, PEGKeywordCategory::KEYWORD_RESERVED)) {
		return KeywordCategory::KEYWORD_RESERVED;
	}
	if (helper.KeywordCategoryType(text, PEGKeywordCategory::KEYWORD_UNRESERVED)) {
		return KeywordCategory::KEYWORD_UNRESERVED;
	}
	if (helper.KeywordCategoryType(text, PEGKeywordCategory::KEYWORD_TYPE_FUNC)) {
		return KeywordCategory::KEYWORD_TYPE_FUNC;
	}
	if (helper.KeywordCategoryType(text, PEGKeywordCategory::KEYWORD_COL_NAME)) {
		return KeywordCategory::KEYWORD_COL_NAME;
	}
	return KeywordCategory::KEYWORD_NONE;
}

bool KeywordHelper::IsKeyword(const string &text, KeywordCategory category) {
	return GetPEGKeywordCategory(text) != category;
}

KeywordCategory KeywordHelper::KeywordCategoryType(const string &text) {
	return GetPEGKeywordCategory(text);
}

bool KeywordHelper::RequiresQuotes(const string &text, bool allow_caps, KeywordCategory category) {
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
	return IsKeyword(text, category);
}

string KeywordHelper::EscapeQuotes(const string &text, char quote) {
	return StringUtil::Replace(text, string(1, quote), string(2, quote));
}

string KeywordHelper::WriteQuoted(const string &text, char quote) {
	// 1. Escapes all occurrences of 'quote' by doubling them (escape in SQL)
	// 2. Adds quotes around the string
	return string(1, quote) + EscapeQuotes(text, quote) + string(1, quote);
}

string KeywordHelper::WriteOptionallyQuoted(const string &text, char quote, bool allow_caps, KeywordCategory category) {
	if (!RequiresQuotes(text, allow_caps, category)) {
		return text;
	}
	return WriteQuoted(text, quote);
}

} // namespace duckdb

#include "duckdb/parser/keyword_extension.hpp"

#include "duckdb/common/enum_util.hpp"
#include "duckdb/parser/peg/keyword_helper.hpp"
#include "duckdb/parser/peg/tokenizer/base_tokenizer.hpp"

namespace duckdb {

static PEGKeywordCategory ToPEGKeywordCategory(ExtensionKeywordCategory category) {
	switch (category) {
	case ExtensionKeywordCategory::RESERVED:
		return PEGKeywordCategory::KEYWORD_RESERVED;
	case ExtensionKeywordCategory::UNRESERVED:
		return PEGKeywordCategory::KEYWORD_UNRESERVED;
	case ExtensionKeywordCategory::FUNCTION_NAME:
		return PEGKeywordCategory::KEYWORD_TYPE_FUNC;
	case ExtensionKeywordCategory::COLUMN_NAME:
		return PEGKeywordCategory::KEYWORD_COL_NAME;
	case ExtensionKeywordCategory::TYPE_NAME:
		return PEGKeywordCategory::KEYWORD_TYPE_NAME;
	default:
		throw InvalidInputException("Invalid extension parser keyword category");
	}
}

static PEGKeywordCategory BuiltinKeywordCategoryType(const string &text) {
	auto &helper = PEGKeywordHelper::Instance();
	if (helper.KeywordCategoryType(text, PEGKeywordCategory::KEYWORD_RESERVED)) {
		return PEGKeywordCategory::KEYWORD_RESERVED;
	}
	if (helper.KeywordCategoryType(text, PEGKeywordCategory::KEYWORD_UNRESERVED)) {
		return PEGKeywordCategory::KEYWORD_UNRESERVED;
	}
	if (helper.KeywordCategoryType(text, PEGKeywordCategory::KEYWORD_TYPE_NAME)) {
		return PEGKeywordCategory::KEYWORD_TYPE_NAME;
	}
	if (helper.KeywordCategoryType(text, PEGKeywordCategory::KEYWORD_TYPE_FUNC)) {
		return PEGKeywordCategory::KEYWORD_TYPE_FUNC;
	}
	if (helper.KeywordCategoryType(text, PEGKeywordCategory::KEYWORD_COL_NAME)) {
		return PEGKeywordCategory::KEYWORD_COL_NAME;
	}
	return PEGKeywordCategory::KEYWORD_NONE;
}

bool KeywordExtension::HasCategory(const string &text, PEGKeywordCategory category) const {
	switch (category) {
	case PEGKeywordCategory::KEYWORD_RESERVED:
		return reserved.find(text) != reserved.end();
	case PEGKeywordCategory::KEYWORD_UNRESERVED:
		return unreserved.find(text) != unreserved.end();
	case PEGKeywordCategory::KEYWORD_TYPE_FUNC:
		return function_name.find(text) != function_name.end();
	case PEGKeywordCategory::KEYWORD_COL_NAME:
		return column_name.find(text) != column_name.end();
	case PEGKeywordCategory::KEYWORD_TYPE_NAME:
		return type_name.find(text) != type_name.end();
	default:
		return false;
	}
}

PEGKeywordCategory KeywordExtension::LookupCategory(const string &text) const {
	if (HasCategory(text, PEGKeywordCategory::KEYWORD_RESERVED)) {
		return PEGKeywordCategory::KEYWORD_RESERVED;
	}
	if (HasCategory(text, PEGKeywordCategory::KEYWORD_UNRESERVED)) {
		return PEGKeywordCategory::KEYWORD_UNRESERVED;
	}
	if (HasCategory(text, PEGKeywordCategory::KEYWORD_TYPE_NAME)) {
		return PEGKeywordCategory::KEYWORD_TYPE_NAME;
	}
	if (HasCategory(text, PEGKeywordCategory::KEYWORD_TYPE_FUNC)) {
		return PEGKeywordCategory::KEYWORD_TYPE_FUNC;
	}
	if (HasCategory(text, PEGKeywordCategory::KEYWORD_COL_NAME)) {
		return PEGKeywordCategory::KEYWORD_COL_NAME;
	}
	return PEGKeywordCategory::KEYWORD_NONE;
}

void KeywordExtension::RegisterKeyword(const ExtensionKeyword &keyword) {
	auto category = ToPEGKeywordCategory(keyword.category);
	const auto &text = keyword.keyword;
	if (PEGKeywordHelper::Instance().KeywordCategoryType(text, category)) {
		return;
	}
	if (text.empty()) {
		throw InvalidInputException("Cannot register an empty parser keyword");
	}
	if (text.size() == 1) {
		throw InvalidInputException("Cannot register single-character parser keyword \"%s\"", text);
	}
	for (auto character : text) {
		if (StringUtil::CharacterIsDigit(character)) {
			throw InvalidInputException("Cannot register parser keyword \"%s\": keywords cannot contain digits", text);
		}
	}
	if (!BaseTokenizer::IsValidUnquotedIdentifier(text)) {
		throw InvalidInputException(
		    "Cannot register parser keyword \"%s\": keywords must be valid unquoted identifiers", text);
	}
	auto existing_category = BuiltinKeywordCategoryType(text);
	if (existing_category == PEGKeywordCategory::KEYWORD_NONE) {
		existing_category = LookupCategory(text);
	}
	if (existing_category != PEGKeywordCategory::KEYWORD_NONE && existing_category != category &&
	    !(existing_category == PEGKeywordCategory::KEYWORD_TYPE_FUNC &&
	      category == PEGKeywordCategory::KEYWORD_TYPE_NAME) &&
	    !(existing_category == PEGKeywordCategory::KEYWORD_TYPE_NAME &&
	      category == PEGKeywordCategory::KEYWORD_TYPE_FUNC)) {
		throw InvalidInputException("Cannot register parser keyword \"%s\" as %s: it is already registered as %s", text,
		                            EnumUtil::ToString(category), EnumUtil::ToString(existing_category));
	}
	switch (category) {
	case PEGKeywordCategory::KEYWORD_RESERVED:
		reserved.insert(text);
		break;
	case PEGKeywordCategory::KEYWORD_UNRESERVED:
		unreserved.insert(text);
		break;
	case PEGKeywordCategory::KEYWORD_TYPE_FUNC:
		function_name.insert(text);
		break;
	case PEGKeywordCategory::KEYWORD_COL_NAME:
		column_name.insert(text);
		break;
	case PEGKeywordCategory::KEYWORD_TYPE_NAME:
		if (!PEGKeywordHelper::Instance().KeywordCategoryType(text, PEGKeywordCategory::KEYWORD_TYPE_FUNC)) {
			function_name.insert(text);
		}
		type_name.insert(text);
		break;
	default:
		throw InternalException("Unexpected parser keyword category");
	}
}

bool KeywordExtension::IsKeyword(const string &text) const {
	return PEGKeywordHelper::Instance().IsKeyword(text) || LookupCategory(text) != PEGKeywordCategory::KEYWORD_NONE;
}

bool KeywordExtension::KeywordCategoryType(const string &text, PEGKeywordCategory category) const {
	return PEGKeywordHelper::Instance().KeywordCategoryType(text, category) || HasCategory(text, category);
}

vector<ParserKeyword> KeywordExtension::KeywordList() const {
	auto result = PEGKeywordHelper::Instance().KeywordList();
	for (const auto &keyword : reserved) {
		result.push_back({keyword, KeywordCategory::KEYWORD_RESERVED});
	}
	for (const auto &keyword : unreserved) {
		result.push_back({keyword, KeywordCategory::KEYWORD_UNRESERVED});
	}
	for (const auto &keyword : function_name) {
		result.push_back({keyword, KeywordCategory::KEYWORD_TYPE_FUNC});
	}
	for (const auto &keyword : column_name) {
		result.push_back({keyword, KeywordCategory::KEYWORD_COL_NAME});
	}
	return result;
}

} // namespace duckdb

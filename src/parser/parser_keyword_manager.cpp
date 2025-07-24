#include "duckdb/parser/parser_keyword_manager.hpp"
#include "duckdb/common/common.hpp"

namespace duckdb {

void ParserKeywordManager::RegisterKeyword(const string &keyword, KeywordCategory category) {
	// Lock the mutex to ensure thread-safe modification of the sets
	std::lock_guard<std::mutex> lock(keyword_mutex);

	switch (category) {
	case KeywordCategory::KEYWORD_RESERVED:
		reserved_keywords.insert(keyword);
		break;
	case KeywordCategory::KEYWORD_UNRESERVED:
		unreserved_keywords.insert(keyword);
		break;
	case KeywordCategory::KEYWORD_TYPE_FUNC:
		type_func_keywords.insert(keyword);
		break;
	case KeywordCategory::KEYWORD_COL_NAME:
		col_name_keywords.insert(keyword);
		break;
	default:
		throw InvalidInputException("Attempted to register keyword \"%s\" with an unrecognized keyword category.",
		                            keyword);
	}
}

bool ParserKeywordManager::IsKeyword(const string &text) const {
	std::lock_guard<std::mutex> lock(keyword_mutex);
	if (reserved_keywords.count(text)) {
		return true;
	}
	if (unreserved_keywords.count(text)) {
		return true;
	}
	if (type_func_keywords.count(text)) {
		return true;
	}
	if (col_name_keywords.count(text)) {
		return true;
	}
	return false;
}

vector<KeywordCategory> ParserKeywordManager::GetKeywordCategories(const string &text) const {
	std::lock_guard<std::mutex> lock(keyword_mutex);
	vector<KeywordCategory> categories;
	if (reserved_keywords.count(text)) {
		categories.push_back(KeywordCategory::KEYWORD_RESERVED);
	}
	if (unreserved_keywords.count(text)) {
		categories.push_back(KeywordCategory::KEYWORD_UNRESERVED);
	}
	if (type_func_keywords.count(text)) {
		categories.push_back(KeywordCategory::KEYWORD_TYPE_FUNC);
	}
	if (col_name_keywords.count(text)) {
		categories.push_back(KeywordCategory::KEYWORD_COL_NAME);
	}
	return categories;
}

bool ParserKeywordManager::IsKeywordInCategory(const string &text, KeywordCategory category) const {
	std::lock_guard<std::mutex> lock(keyword_mutex);
	switch (category) {
	case KeywordCategory::KEYWORD_RESERVED:
		return reserved_keywords.count(text);
	case KeywordCategory::KEYWORD_UNRESERVED:
		return unreserved_keywords.count(text);
	case KeywordCategory::KEYWORD_TYPE_FUNC:
		return type_func_keywords.count(text);
	case KeywordCategory::KEYWORD_COL_NAME:
		return col_name_keywords.count(text);
	default:
		return false;
	}
}

} // namespace duckdb

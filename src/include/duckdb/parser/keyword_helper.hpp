//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/keyword_helper.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/parser/simplified_token.hpp"

namespace duckdb {

class KeywordHelper {
public:
	//! Returns true if the given text matches a keyword of the parser
	static bool IsKeyword(const string &text, KeywordCategory category = KeywordCategory::KEYWORD_NONE);

	static KeywordCategory KeywordCategoryType(const string &text);

	//! Returns true if the given string needs to be quoted when written as an identifier
	static bool RequiresQuotes(const string &text, bool allow_caps = true);

	//! Writes a string that is quoted
	[[deprecated("This function has been deprecated due to it having confusing syntax, use SQLString instead for "
	             "literals, or SQLIdentifier/SQLQuotedIdentifier for identifiers")]] static string
	WriteQuoted(const string &text, char quote = '\'');

	//! Writes a string that is optionally quoted + escaped so it can be used as an identifier
	[[deprecated("This function has been deprecated due to it having confusing syntax, use "
	             "SQLIdentifier/SQLQuotedIdentifier instead for identifiers, or SQLString for literals")]] static string
	WriteOptionallyQuoted(const string &text, char quote = '"', bool allow_caps = true);

	static string WriteQuotedAndEscaped(const string &text, char quote);
};

} // namespace duckdb

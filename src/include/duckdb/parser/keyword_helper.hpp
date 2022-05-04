//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/keyword_helper.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

class KeywordHelper {
public:
	//! Returns true if the given text matches a keyword of the parser
	static bool IsKeyword(const string &text);

	//! Returns true if the given string needs to be quoted when written as an identifier
	static bool RequiresQuotes(const string &text);

	//! Writes a string that is optionally quoted + escaped so it can be used as an identifier
	static string WriteOptionallyQuoted(const string &text, char quote = '"');
};

} // namespace duckdb

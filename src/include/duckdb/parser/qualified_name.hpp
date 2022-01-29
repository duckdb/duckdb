//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/qualified_name.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

struct QualifiedName {
	string schema;
	string name;

	//! Quote and escape the input as required to make the input parse-able in SQL as an identifier
	static string Quote(const string &input) {
		D_ASSERT(!input.empty());
		bool contains_quote = false;
		bool must_be_quoted = false;
		for (idx_t i = 0; i < input.size(); i++) {
			if (input[i] == '"') {
				contains_quote = true;
			} else if (!StringUtil::CharacterIsAlpha(input[i])) {
				must_be_quoted = true;
			}
		}
		string result;
		if (contains_quote) {
			// need to escape existing quotes
			result = StringUtil::Replace(input, "\"", "\"\"");
		} else {
			result = input;
		}
		if (must_be_quoted || KeywordHelper::IsKeyword(input)) {
			return "\"" + result + "\"";
		} else {
			return result;
		}
	}

	//! Parse the (optional) schema and a name from a string in the format of e.g. "schema"."table"; if there is no dot
	//! the schema will be set to INVALID_SCHEMA
	static QualifiedName Parse(const string &input) {
		string schema;
		string name;
		idx_t idx = 0;
		vector<string> entries;
		string entry;
	normal:
		//! quote
		for (; idx < input.size(); idx++) {
			if (input[idx] == '"') {
				idx++;
				goto quoted;
			} else if (input[idx] == '.') {
				goto separator;
			}
			entry += input[idx];
		}
		goto end;
	separator:
		entries.push_back(entry);
		entry = "";
		idx++;
		goto normal;
	quoted:
		//! look for another quote
		for (; idx < input.size(); idx++) {
			if (input[idx] == '"') {
				//! unquote
				idx++;
				goto normal;
			}
			entry += input[idx];
		}
		throw ParserException("Unterminated quote in qualified name!");
	end:
		if (entries.empty()) {
			schema = INVALID_SCHEMA;
			name = entry;
		} else if (entries.size() == 1) {
			schema = entries[0];
			name = entry;
		} else {
			throw ParserException("Expected schema.entry or entry: too many entries found");
		}
		return QualifiedName {schema, name};
	}
};

struct QualifiedColumnName {
	QualifiedColumnName() {
	}
	QualifiedColumnName(string table_p, string column_p) : table(move(table_p)), column(move(column_p)) {
	}

	string schema;
	string table;
	string column;
};

} // namespace duckdb

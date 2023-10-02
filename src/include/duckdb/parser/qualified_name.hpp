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
	string catalog;
	string schema;
	string name;

	//! Parse the (optional) schema and a name from a string in the format of e.g. "schema"."table"; if there is no dot
	//! the schema will be set to INVALID_SCHEMA
	static QualifiedName Parse(const string &input) {
		string catalog;
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
			catalog = INVALID_CATALOG;
			schema = INVALID_SCHEMA;
			name = entry;
		} else if (entries.size() == 1) {
			catalog = INVALID_CATALOG;
			schema = entries[0];
			name = entry;
		} else if (entries.size() == 2) {
			catalog = entries[0];
			schema = entries[1];
			name = entry;
		} else {
			throw ParserException("Expected catalog.entry, schema.entry or entry: too many entries found");
		}
		return QualifiedName {catalog, schema, name};
	}
};

struct QualifiedColumnName {
	QualifiedColumnName() {
	}
	QualifiedColumnName(string table_p, string column_p) : table(std::move(table_p)), column(std::move(column_p)) {
	}

	string schema;
	string table;
	string column;
};

} // namespace duckdb

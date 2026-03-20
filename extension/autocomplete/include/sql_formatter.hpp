//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sql_formatter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/typedefs.hpp"

namespace duckdb {

//! Configuration for the SQL pretty-printer.
struct FormatterConfig {
	//! Number of spaces per indentation level.
	idx_t indent_size = 4;

	//! Maximum character width of a clause keyword + its content when placed on
	//! a single line.  Clauses whose content fits within this limit are inlined
	//! (e.g. "FROM orders" instead of "FROM\n    orders").
	//! Set to 0 to disable inlining and always emit multiline output.
	idx_t inline_threshold = 60;
};

//! Pretty-prints a SQL string using the PEG tokenizer for lexical analysis.
//! Keywords are uppercased; major clause keywords (SELECT, FROM, WHERE, etc.)
//! are placed on their own lines; short clauses are optionally inlined based on
//! FormatterConfig::inline_threshold.
string FormatSQL(const string &sql, const FormatterConfig &config = FormatterConfig {});

} // namespace duckdb

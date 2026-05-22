#pragma once
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/parser/constraint.hpp"
#include "duckdb/parser/parsed_expression.hpp"

namespace duckdb {
struct ColumnElements {
	ColumnList columns;
	vector<unique_ptr<Constraint>> constraints;
	vector<unique_ptr<ParsedExpression>> partition_keys;
	vector<unique_ptr<ParsedExpression>> sort_keys;
	case_insensitive_map_t<unique_ptr<ParsedExpression>> options;
	// PG-compat: column names that mixed an explicit NULL with NOT NULL /
	// PRIMARY KEY in their definition. The CreateTable transformer reads
	// these out and raises a single error per column with the table name.
	vector<string> null_conflict_columns;
	// PG-compat: column names that declared
	// DEFAULT more than once. Same deferred-error pattern as the null conflict
	// list above; the CreateTable transformer raises the canonical PG message
	// once the table name is known.
	vector<string> duplicate_default_columns;
};
} // namespace duckdb

#pragma once
#include "duckdb/common/common.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/constraint.hpp"

namespace duckdb {
struct GeneratedColumnDefinition {
	unique_ptr<ParsedExpression> expr;
	bool virtual_column = false;
	bool default_column = false;
	//! True when the column was declared as STORED, false for VIRTUAL (default).
	bool stored = false;
};

struct ConstraintColumnDefinition {
	ColumnDefinition column_definition;
	vector<pair<bool, ConstraintType>> constraint_types;
	vector<unique_ptr<Constraint>> constraints;
	// PG-compat: tracked so the CreateTable transformer can emit the
	// "conflicting NULL/NOT NULL declarations for column X of table Y"
	// error after the table name is known. Set true when the grammar saw
	// a bare 'NULL' (NotNullConstraint with no 'NOT' prefix).
	bool has_explicit_null = false;
	bool has_not_null = false;
	bool has_primary_key = false;
	// PG-compat: column declared DEFAULT more than once. Surfaced by the
	// CreateTable transformer (which knows the table name).
	bool has_duplicate_default = false;
};
} // namespace duckdb

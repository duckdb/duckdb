#pragma once
#include "duckdb/common/common.hpp"

namespace duckdb {
struct GeneratedColumnDefinition {
	unique_ptr<ParsedExpression> expr;
	bool virtual_column = false;
	bool default_column = false;
};

struct ConstraintColumnDefinition {
	ColumnDefinition column_definition;
	vector<pair<bool, ConstraintType>> constraint_types;
	vector<unique_ptr<Constraint>> constraints;
};
} // namespace duckdb

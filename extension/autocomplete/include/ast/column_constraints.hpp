#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/parser/constraint.hpp"

namespace duckdb {

struct ColumnConstraint {
	vector<unique_ptr<Constraint>> constraints;
	vector<pair<bool, ConstraintType>> constraint_types; // Used to create proper constrains when column index is known
	unique_ptr<ParsedExpression> default_value;
};

} // namespace duckdb

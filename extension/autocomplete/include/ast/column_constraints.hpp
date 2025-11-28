#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/parser/constraint.hpp"

namespace duckdb {

struct ColumnConstraint {
	vector<unique_ptr<Constraint>> constraints;
	unique_ptr<ParsedExpression> default_value;
};

} // namespace duckdb

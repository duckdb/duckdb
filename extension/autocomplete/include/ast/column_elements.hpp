#pragma once
#include "duckdb/common/vector.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/parser/constraint.hpp"

namespace duckdb {
struct ColumnElements {
	ColumnList columns;
	vector<unique_ptr<Constraint>> constraints;
};
} // namespace duckdb

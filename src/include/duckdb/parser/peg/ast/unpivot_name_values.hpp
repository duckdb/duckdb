#pragma once
#include "duckdb/parser/tableref/pivotref.hpp"

namespace duckdb {
struct UnpivotNameValues {
	vector<Identifier> unpivot_names;
	PivotColumn column;
};
} // namespace duckdb

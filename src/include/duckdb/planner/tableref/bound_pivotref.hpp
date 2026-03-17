//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/tableref/bound_pivotref.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/parser/tableref/pivotref.hpp"
#include "duckdb/function/aggregate_function.hpp"

namespace duckdb {

struct BoundPivotInfo {
	//! The number of group columns
	idx_t group_count;
	//! The set of types
	vector<LogicalType> types;
	//! The set of values to pivot on
	vector<string> pivot_values;
	//! The set of aggregate functions that is being executed
	vector<unique_ptr<Expression>> aggregates;

	void Serialize(Serializer &serializer) const;
	static BoundPivotInfo Deserialize(Deserializer &deserializer);
};

class BoundPivotRef {
public:
	idx_t bind_index;
	//! The binder used to bind the child of the pivot
	shared_ptr<Binder> child_binder;
	//! The child node of the pivot
	BoundStatement child;
	//! The bound pivot info
	BoundPivotInfo bound_pivot;
};
} // namespace duckdb

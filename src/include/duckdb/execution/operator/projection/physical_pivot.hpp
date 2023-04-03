//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/projection/physical_pivot.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/common/string_map_set.hpp"
#include "duckdb/planner/tableref/bound_pivotref.hpp"

namespace duckdb {

//! PhysicalPivot implements the physical PIVOT operation
class PhysicalPivot : public PhysicalOperator {
public:
	PhysicalPivot(vector<LogicalType> types, unique_ptr<PhysicalOperator> child, BoundPivotInfo bound_pivot);

	BoundPivotInfo bound_pivot;
	//! The map for pivot value -> column index
	string_map_t<idx_t> pivot_map;
	//! The empty aggregate values
	vector<Value> empty_aggregates;

public:
	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                           GlobalOperatorState &gstate, OperatorState &state) const override;

	bool ParallelOperator() const override {
		return true;
	}
};

} // namespace duckdb

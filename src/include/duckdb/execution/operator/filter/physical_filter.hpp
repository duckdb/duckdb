//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/filter/physical_filter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

//! PhysicalFilter represents a filter operator. It removes non-matching tuples
//! from the result. Note that it does not physically change the data, it only
//! adds a selection vector to the chunk.
class PhysicalFilter : public PhysicalOperator {
public:
	PhysicalFilter(vector<LogicalType> types, vector<unique_ptr<Expression>> select_list, idx_t estimated_cardinality);

	//! The filter expression
	unique_ptr<Expression> expression;

public:
	void GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) const override;

	unique_ptr<PhysicalOperatorState> GetOperatorState() override;
	string ParamsToString() const override;
	void FinalizeOperatorState(PhysicalOperatorState &state_p, ExecutionContext &context) override;
};
} // namespace duckdb

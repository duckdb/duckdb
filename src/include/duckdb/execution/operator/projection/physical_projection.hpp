//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/projection/physical_projection.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

class PhysicalProjection : public PhysicalOperator {
public:
	PhysicalProjection(vector<LogicalType> types, vector<unique_ptr<Expression>> select_list,
	                   idx_t estimated_cardinality)
	    : PhysicalOperator(PhysicalOperatorType::PROJECTION, move(types), estimated_cardinality),
	      select_list(move(select_list)) {
	}

	vector<unique_ptr<Expression>> select_list;

public:
	void GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) const override;

	unique_ptr<PhysicalOperatorState> GetOperatorState() override;
	void FinalizeOperatorState(PhysicalOperatorState &state, ExecutionContext &context) override;

	string ParamsToString() const override;
};

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/set/physical_union.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {
class PhysicalUnion : public PhysicalOperator {
public:
	PhysicalUnion(vector<LogicalType> types, unique_ptr<PhysicalOperator> top, unique_ptr<PhysicalOperator> bottom,
	              idx_t estimated_cardinality);

// public:
// 	void GetChunkInternal(ExecutionContext &context, DataChunk &chunk, OperatorState *state) const override;
// 	unique_ptr<OperatorState> GetOperatorState() override;
// 	void FinalizeOperatorState(OperatorState &state_p, ExecutionContext &context) override;
};

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/helper/physical_streaming_limit.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/bound_result_modifier.hpp"

namespace duckdb {

class PhysicalStreamingLimit : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::STREAMING_LIMIT;

public:
	PhysicalStreamingLimit(vector<LogicalType> types, BoundLimitNode limit_val_p, BoundLimitNode offset_val_p,
	                       idx_t estimated_cardinality, bool parallel);

	BoundLimitNode limit_val;
	BoundLimitNode offset_val;
	bool parallel;

public:
	// Operator interface
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;
	unique_ptr<GlobalOperatorState> GetGlobalOperatorState(ClientContext &context) const override;
	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                           GlobalOperatorState &gstate, OperatorState &state) const override;

	OrderPreservationType OperatorOrder() const override;
	bool ParallelOperator() const override;
};

} // namespace duckdb

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

namespace duckdb {

class PhysicalStreamingLimit : public PhysicalOperator {
public:
	PhysicalStreamingLimit(vector<LogicalType> types, idx_t limit, idx_t offset,
	                       unique_ptr<Expression> limit_expression, unique_ptr<Expression> offset_expression,
	                       idx_t estimated_cardinality, bool parallel);

	idx_t limit_value;
	idx_t offset_value;
	unique_ptr<Expression> limit_expression;
	unique_ptr<Expression> offset_expression;
	bool parallel;

public:
	// Operator interface
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;
	unique_ptr<GlobalOperatorState> GetGlobalOperatorState(ClientContext &context) const override;
	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                           GlobalOperatorState &gstate, OperatorState &state) const override;

	bool IsOrderDependent() const override;
	bool ParallelOperator() const override;
};

} // namespace duckdb

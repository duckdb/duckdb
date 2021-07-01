//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/helper/physical_limit.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
namespace duckdb {

//! PhyisicalLimit represents the LIMIT operator
class PhysicalLimit : public PhysicalOperator {
public:
	PhysicalLimit(vector<LogicalType> types, idx_t limit, idx_t offset, unique_ptr<Expression> limit_expression,
	              unique_ptr<Expression> offset_expression, idx_t estimated_cardinality)
	    : PhysicalOperator(PhysicalOperatorType::LIMIT, move(types), estimated_cardinality), limit_value(limit),
	      offset_value(offset), limit_expression(move(limit_expression)), offset_expression(move(offset_expression)) {
	}

	idx_t limit_value;
	idx_t offset_value;
	unique_ptr<Expression> limit_expression;
	unique_ptr<Expression> offset_expression;

public:
	void GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) const override;

	unique_ptr<PhysicalOperatorState> GetOperatorState() override;
};

} // namespace duckdb

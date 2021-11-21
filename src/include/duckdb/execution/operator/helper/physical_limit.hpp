//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/helper/physical_limit.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

//! PhyisicalLimit represents the LIMIT operator
class PhysicalLimit : public PhysicalOperator {
public:
	PhysicalLimit(vector<LogicalType> types, idx_t limit, idx_t offset, unique_ptr<Expression> limit_expression,
	              unique_ptr<Expression> offset_expression, idx_t estimated_cardinality)
	    : PhysicalOperator(PhysicalOperatorType::LIMIT, move(types), estimated_cardinality), is_limit_percent(false),
	      limit_value(limit), offset_value(offset), limit_expression(move(limit_expression)),
	      offset_expression(move(offset_expression)) {
	}

	PhysicalLimit(vector<LogicalType> types, double limit_percent, idx_t offset,
	              unique_ptr<Expression> limit_expression, unique_ptr<Expression> offset_expression,
	              idx_t estimated_cardinality)
	    : PhysicalOperator(PhysicalOperatorType::LIMIT, move(types), estimated_cardinality),
	      is_limit_percent(true), limit_percent(limit_percent), offset_value(offset),
	      limit_expression(move(limit_expression)), offset_expression(move(offset_expression)) {
	}

	idx_t limit_value = INVALID_INDEX;
	bool is_limit_percent = false;
	double limit_percent = 100.0;
	idx_t limit_count = INVALID_INDEX;
	idx_t offset_value;
	unique_ptr<Expression> limit_expression;
	unique_ptr<Expression> offset_expression;

public:
	// Source interface
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	void GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
	             LocalSourceState &lstate) const override;

public:
	// Sink Interface
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate,
	                    DataChunk &input) const override;

	bool IsSink() const override {
		return true;
	}

	bool SinkOrderMatters() const override {
		return true;
	}
};

} // namespace duckdb

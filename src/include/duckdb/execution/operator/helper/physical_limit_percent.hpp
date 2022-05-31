//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/helper/physical_limit_percent.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

//! PhyisicalLimitPercent represents the LIMIT PERCENT operator
class PhysicalLimitPercent : public PhysicalOperator {
public:
	PhysicalLimitPercent(vector<LogicalType> types, double limit_percent, idx_t offset,
	                     unique_ptr<Expression> limit_expression, unique_ptr<Expression> offset_expression,
	                     idx_t estimated_cardinality)
	    : PhysicalOperator(PhysicalOperatorType::LIMIT_PERCENT, move(types), estimated_cardinality),
	      limit_percent(limit_percent), offset_value(offset), limit_expression(move(limit_expression)),
	      offset_expression(move(offset_expression)) {
	}

	double limit_percent;
	idx_t offset_value;
	unique_ptr<Expression> limit_expression;
	unique_ptr<Expression> offset_expression;

public:
	bool IsOrderDependent() const override {
		return true;
	}

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
};

} // namespace duckdb

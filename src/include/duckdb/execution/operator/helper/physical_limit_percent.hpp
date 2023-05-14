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
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::LIMIT_PERCENT;

public:
	PhysicalLimitPercent(vector<LogicalType> types, double limit_percent, idx_t offset,
	                     unique_ptr<Expression> limit_expression, unique_ptr<Expression> offset_expression,
	                     idx_t estimated_cardinality)
	    : PhysicalOperator(PhysicalOperatorType::LIMIT_PERCENT, std::move(types), estimated_cardinality),
	      limit_percent(limit_percent), offset_value(offset), limit_expression(std::move(limit_expression)),
	      offset_expression(std::move(offset_expression)) {
	}

	double limit_percent;
	idx_t offset_value;
	unique_ptr<Expression> limit_expression;
	unique_ptr<Expression> offset_expression;

public:
	bool SinkOrderDependent() const override {
		return true;
	}

public:
	// Source interface
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}

public:
	// Sink Interface
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;

	bool IsSink() const override {
		return true;
	}
};

} // namespace duckdb

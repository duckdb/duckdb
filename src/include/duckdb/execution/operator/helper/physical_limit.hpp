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
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::LIMIT;

public:
	PhysicalLimit(vector<LogicalType> types, idx_t limit, idx_t offset, unique_ptr<Expression> limit_expression,
	              unique_ptr<Expression> offset_expression, idx_t estimated_cardinality);

	idx_t limit_value;
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
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;

	bool IsSink() const override {
		return true;
	}

	bool ParallelSink() const override {
		return true;
	}

	bool RequiresBatchIndex() const override {
		return true;
	}

public:
	static bool ComputeOffset(ExecutionContext &context, DataChunk &input, idx_t &limit, idx_t &offset,
	                          idx_t current_offset, idx_t &max_element, Expression *limit_expression,
	                          Expression *offset_expression);
	static bool HandleOffset(DataChunk &input, idx_t &current_offset, idx_t offset, idx_t limit);
	static Value GetDelimiter(ExecutionContext &context, DataChunk &input, Expression *expr);
};

} // namespace duckdb

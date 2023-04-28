//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/projection/physical_time_range.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

//! PhysicalTimeRange implements the physical RANGE operation on timestamps
class PhysicalTimeRange : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::TIME_RANGE;

public:
	PhysicalTimeRange(vector<LogicalType> types, vector<unique_ptr<Expression>> args_list, idx_t estimated_cardinality,
	               PhysicalOperatorType type = PhysicalOperatorType::TIME_RANGE);

	vector<unique_ptr<Expression>> args_list;
	bool generate_series;

public:
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;
	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                           GlobalOperatorState &gstate, OperatorState &state) const override;

	bool ParallelOperator() const override {
		return true;
	}

public:
	static unique_ptr<OperatorState> GetState(ExecutionContext &context,
	                                          const vector<unique_ptr<Expression>> &select_list,
											  bool generate_series);
	//! Executes the TIME_RANGE operator internally and emits a chunk of sequence data.
	static OperatorResultType ExecuteInternal(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                                          OperatorState &state, const vector<unique_ptr<Expression>> &args_list);
};

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/projection/physical_range.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

//! PhysicalRange implements the physical RANGE operation on integers
class PhysicalRange : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::RANGE;

public:
	PhysicalRange(vector<LogicalType> types, vector<unique_ptr<Expression>> args_list, idx_t estimated_cardinality,
	              PhysicalOperatorType type = PhysicalOperatorType::RANGE);

	// args_list and generate_series are never instantiated,
	// since the PhysicalRange constructor is apparently never called.
	// They are declared here only to enable implemention of GetOperatorState.
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
	                                          const vector<unique_ptr<Expression>> &select_list, bool generate_series);
	//! Executes the RANGE operator internally and emits a chunk of sequence data.
	static OperatorResultType ExecuteInternal(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                                          OperatorState &state, const vector<unique_ptr<Expression>> &args_list);
};

} // namespace duckdb

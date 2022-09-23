//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/aggregate/physical_window.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

//! PhysicalStreamingWindow implements streaming window functions (i.e. with an empty OVER clause)
class PhysicalStreamingWindow : public PhysicalOperator {
public:
	PhysicalStreamingWindow(vector<LogicalType> types, vector<unique_ptr<Expression>> select_list,
	                        idx_t estimated_cardinality,
	                        PhysicalOperatorType type = PhysicalOperatorType::STREAMING_WINDOW);

	//! The projection list of the WINDOW statement
	vector<unique_ptr<Expression>> select_list;

public:
	unique_ptr<GlobalOperatorState> GetGlobalOperatorState(ClientContext &context) const override;
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;

	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                           GlobalOperatorState &gstate, OperatorState &state) const override;

	string ParamsToString() const override;
};

} // namespace duckdb

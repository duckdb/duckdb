//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/operator/aggregate/physical_window.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/physical_operator.hpp"

namespace duckdb {

//! PhysicalAggregate represents a group-by and aggregation operator. Note that
//! it is an abstract class, its implementation is not defined here.
class PhysicalWindow : public PhysicalOperator {
public:
	PhysicalWindow(LogicalOperator &op, vector<unique_ptr<Expression>> select_list,
	                  PhysicalOperatorType type = PhysicalOperatorType::WINDOW);

	void Initialize();

	void _GetChunk(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;

	//! The projection list of the SELECT statement (that contains aggregates)
	vector<unique_ptr<Expression>> select_list;
};

//! The operator state of the aggregate
class PhysicalWindowOperatorState : public PhysicalOperatorState {
public:
	PhysicalWindowOperatorState(PhysicalWindow *parent, PhysicalOperator *child = nullptr,
	                               ExpressionExecutor *parent_executor = nullptr);

	// TODO
};

} // namespace duckdb

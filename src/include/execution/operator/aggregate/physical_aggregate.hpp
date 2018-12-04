//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/operator/aggregate/physical_aggregate.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/physical_operator.hpp"

namespace duckdb {

//! PhysicalAggregate represents a group-by and aggregation operator. Note that
//! it is an abstract class, its implementation is not defined here.
class PhysicalAggregate : public PhysicalOperator {
	public:
	PhysicalAggregate(LogicalOperator &op, std::vector<std::unique_ptr<Expression>> select_list,
	                  PhysicalOperatorType type = PhysicalOperatorType::BASE_GROUP_BY);
	PhysicalAggregate(LogicalOperator &op, std::vector<std::unique_ptr<Expression>> select_list,
	                  std::vector<std::unique_ptr<Expression>> groups,
	                  PhysicalOperatorType type = PhysicalOperatorType::BASE_GROUP_BY);

	void Initialize();

	//! The projection list of the SELECT statement (that contains aggregates)
	std::vector<std::unique_ptr<Expression>> select_list;
	//! The groups
	std::vector<std::unique_ptr<Expression>> groups;
	//! The actual aggregates that have to be computed (i.e. the deepest
	//! aggregates in the expression)
	std::vector<AggregateExpression *> aggregates;
	bool is_implicit_aggr;
};

//! The operator state of the aggregate
class PhysicalAggregateOperatorState : public PhysicalOperatorState {
	public:
	PhysicalAggregateOperatorState(PhysicalAggregate *parent, PhysicalOperator *child = nullptr,
	                               ExpressionExecutor *parent_executor = nullptr);

	//! Aggregate values, used only for aggregates without GROUP BY
	std::vector<Value> aggregates;
	//! Materialized GROUP BY expression
	DataChunk group_chunk;
	//! Materialized aggregates
	DataChunk aggregate_chunk;
};

} // namespace duckdb

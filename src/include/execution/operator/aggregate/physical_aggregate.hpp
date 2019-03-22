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
	PhysicalAggregate(vector<TypeId> types, vector<unique_ptr<Expression>> select_list,
	                  PhysicalOperatorType type = PhysicalOperatorType::BASE_GROUP_BY);
	PhysicalAggregate(vector<TypeId> types, vector<unique_ptr<Expression>> select_list,
	                  vector<unique_ptr<Expression>> groups,
	                  PhysicalOperatorType type = PhysicalOperatorType::BASE_GROUP_BY);

	//! The groups
	vector<unique_ptr<Expression>> groups;
	//! The aggregates that have to be computed
	vector<unique_ptr<Expression>> aggregates;
	bool is_implicit_aggr;
};

//! The operator state of the aggregate
class PhysicalAggregateOperatorState : public PhysicalOperatorState {
public:
	PhysicalAggregateOperatorState(PhysicalAggregate *parent, PhysicalOperator *child = nullptr);

	//! Aggregate values, used only for aggregates without GROUP BY
	vector<Value> aggregates;
	//! Materialized GROUP BY expression
	DataChunk group_chunk;
	//! Materialized aggregates
	DataChunk aggregate_chunk;
};

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/aggregate/grouped_aggregate_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/parser/group_by_node.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"

namespace duckdb {

class GroupedAggregateData {
public:
	GroupedAggregateData() {
	}
	//! The groups
	vector<unique_ptr<Expression>> groups;
	//! The set of GROUPING functions
	vector<vector<idx_t>> grouping_functions;
	//! The group types
	vector<LogicalType> group_types;

	//! The aggregates that have to be computed
	vector<unique_ptr<Expression>> aggregates;
	//! The payload types
	vector<LogicalType> payload_types;
	//! The aggregate return types
	vector<LogicalType> aggregate_return_types;
	//! Pointers to the aggregates
	vector<BoundAggregateExpression *> bindings;
	idx_t filter_count;

public:
	idx_t GroupCount() const;

	const vector<vector<idx_t>> &GetGroupingFunctions() const;

	void InitializeGroupby(vector<unique_ptr<Expression>> groups, vector<unique_ptr<Expression>> expressions,
	                       vector<unsafe_vector<idx_t>> grouping_functions);

	//! Initialize a GroupedAggregateData object for use with distinct aggregates
	void InitializeDistinct(const unique_ptr<Expression> &aggregate, const vector<unique_ptr<Expression>> *groups_p);

private:
	void InitializeDistinctGroups(const vector<unique_ptr<Expression>> *groups);
	void InitializeGroupbyGroups(vector<unique_ptr<Expression>> groups);
	void SetGroupingFunctions(vector<unsafe_vector<idx_t>> &functions);
};

} // namespace duckdb

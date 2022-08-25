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
	//! Whether or not any aggregation is DISTINCT
	bool any_distinct = false;
	//! The payload types
	vector<LogicalType> payload_types;
	//! The aggregate return types
	vector<LogicalType> aggregate_return_types;
	//! Pointers to the aggregates
	vector<BoundAggregateExpression *> bindings;

public:
	idx_t GroupCount() const {
		return groups.size();
	}

	const vector<vector<idx_t>> &GetGroupingFunctions() const {
		return grouping_functions;
	}

	void InitializeGroupby(vector<unique_ptr<Expression>> groups, vector<unique_ptr<Expression>> expressions,
	                       vector<vector<idx_t>> grouping_functions) {
		InitializeGroupbyGroups(move(groups));
		vector<LogicalType> payload_types_filters;

		SetGroupingFunctions(grouping_functions);

		for (auto &expr : expressions) {
			D_ASSERT(expr->expression_class == ExpressionClass::BOUND_AGGREGATE);
			D_ASSERT(expr->IsAggregate());
			auto &aggr = (BoundAggregateExpression &)*expr;
			bindings.push_back(&aggr);

			if (aggr.distinct) {
				any_distinct = true;
			}

			aggregate_return_types.push_back(aggr.return_type);
			for (auto &child : aggr.children) {
				payload_types.push_back(child->return_type);
			}
			if (aggr.filter) {
				payload_types_filters.push_back(aggr.filter->return_type);
			}
			if (!aggr.function.combine) {
				throw InternalException("Aggregate function %s is missing a combine method", aggr.function.name);
			}
			aggregates.push_back(move(expr));
		}
		for (const auto &pay_filters : payload_types_filters) {
			payload_types.push_back(pay_filters);
		}
	}
	//! Initialize a GroupedAggregateData object for use with distinct aggregates
	void InitializeDistinct(const unique_ptr<Expression> &aggregate) {
		auto &aggr = (BoundAggregateExpression &)*aggregate;
		D_ASSERT(aggr.distinct);
		any_distinct = false; //! This is done to stop the radixHT from enforcing ForceSingleHT

		vector<LogicalType> payload_types_filters;
		aggregate_return_types.push_back(aggr.return_type);
		for (idx_t i = 0; i < aggr.children.size(); i++) {
			auto &child = aggr.children[i];
			group_types.push_back(child->return_type);
			groups.push_back(child->Copy());
		}
		if (!aggr.function.combine) {
			throw InternalException("Aggregate function %s is missing a combine method", aggr.function.name);
		}
	}

private:
	void InitializeGroupbyGroups(vector<unique_ptr<Expression>> groups) {
		// Add all the expressions of the group by clause
		for (auto &expr : groups) {
			group_types.push_back(expr->return_type);
		}
		this->groups = move(groups);
	}

	void SetGroupingFunctions(vector<vector<idx_t>> &functions) {
		grouping_functions.reserve(functions.size());
		for (idx_t i = 0; i < functions.size(); i++) {
			grouping_functions.push_back(move(functions[i]));
		}
	}
};

} // namespace duckdb

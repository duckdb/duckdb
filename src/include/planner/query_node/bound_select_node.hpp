//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/query_node/bound_select_node.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/bound_query_node.hpp"
#include "planner/bound_tableref.hpp"

namespace duckdb {

//! Bound equivalent of SelectNode
class BoundSelectNode : public BoundQueryNode {
public:
	BoundSelectNode() : BoundQueryNode(QueryNodeType::SELECT_NODE) {
	}

	//! The projection list
	vector<unique_ptr<Expression>> select_list;
	//! The FROM clause
	unique_ptr<BoundTableRef> from_table;
	//! The WHERE clause
	unique_ptr<Expression> where_clause;
	//! list of groups
	vector<unique_ptr<Expression>> groups;
	//! HAVING clause
	unique_ptr<Expression> having;

	//! The amount of columns in the final result
	uint64_t column_count;

	//! Index used by the LogicalProjection
	uint64_t projection_index;

	//! Group index used by the LogicalAggregate (only used if HasAggregation is true)
	uint64_t group_index;
	//! Aggregate index used by the LogicalAggregate (only used if HasAggregation is true)
	uint64_t aggregate_index;
	//! Aggregate functions to compute (only used if HasAggregation is true)
	vector<unique_ptr<Expression>> aggregates;

	//! Window index used by the LogicalWindow (only used if HasWindow is true)
	uint64_t window_index;
	//! Window functions to compute (only used if HasWindow is true)
	vector<unique_ptr<Expression>> windows;

	const vector<unique_ptr<Expression>> &GetSelectList() const override {
		return select_list;
	}
};
}; // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/query_node/bound_select_node.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/bound_query_node.hpp"
#include "duckdb/planner/bound_tableref.hpp"
#include "duckdb/parser/expression_map.hpp"

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
	index_t column_count;

	//! Index used by the LogicalProjection
	index_t projection_index;

	//! Group index used by the LogicalAggregate (only used if HasAggregation is true)
	index_t group_index;
	//! Aggregate index used by the LogicalAggregate (only used if HasAggregation is true)
	index_t aggregate_index;
	//! Aggregate functions to compute (only used if HasAggregation is true)
	vector<unique_ptr<Expression>> aggregates;
	//! Map from aggregate function to aggregate index (used to eliminate duplicate aggregates)
	expression_map_t<index_t> aggregate_map;

	//! Window index used by the LogicalWindow (only used if HasWindow is true)
	index_t window_index;
	//! Window functions to compute (only used if HasWindow is true)
	vector<unique_ptr<Expression>> windows;

public:
	index_t GetRootIndex() override {
		return projection_index;
	}
};
}; // namespace duckdb

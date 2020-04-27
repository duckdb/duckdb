//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/query_node/bound_select_node.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/bound_query_node.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/parser/expression_map.hpp"
#include "duckdb/planner/bound_tableref.hpp"

namespace duckdb {

//! Bound equivalent of SelectNode
class BoundSelectNode : public BoundQueryNode {
public:
	BoundSelectNode() : BoundQueryNode(QueryNodeType::SELECT_NODE) {
	}

	//! The original unparsed expressions. This is exported after binding, because the binding might change the
	//! expressions (e.g. when a * clause is present)
	vector<unique_ptr<ParsedExpression>> original_expressions;

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
	idx_t column_count;

	//! Index used by the LogicalProjection
	idx_t projection_index;

	//! Group index used by the LogicalAggregate (only used if HasAggregation is true)
	idx_t group_index;
	//! Aggregate index used by the LogicalAggregate (only used if HasAggregation is true)
	idx_t aggregate_index;
	//! Aggregate functions to compute (only used if HasAggregation is true)
	vector<unique_ptr<Expression>> aggregates;

	//! Map from aggregate function to aggregate index (used to eliminate duplicate aggregates)
	expression_map_t<idx_t> aggregate_map;

	//! Window index used by the LogicalWindow (only used if HasWindow is true)
	idx_t window_index;
	//! Window functions to compute (only used if HasWindow is true)
	vector<unique_ptr<Expression>> windows;

	idx_t unnest_index;
	//! Unnest expression
	vector<unique_ptr<Expression>> unnests;

	//! Index of pruned node
	idx_t prune_index;
	bool need_prune = false;

public:
	idx_t GetRootIndex() override {
		return need_prune ? prune_index : projection_index;
	}
};
}; // namespace duckdb

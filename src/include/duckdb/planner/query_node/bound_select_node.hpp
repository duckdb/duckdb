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
#include "duckdb/parser/parsed_data/sample_options.hpp"
#include "duckdb/parser/group_by_node.hpp"
#include "duckdb/planner/expression_binder/select_bind_state.hpp"

namespace duckdb {

class BoundGroupByNode {
public:
	//! The total set of all group expressions
	vector<unique_ptr<Expression>> group_expressions;
	//! The different grouping sets as they map to the group expressions
	vector<GroupingSet> grouping_sets;
};

struct BoundUnnestNode {
	//! The index of the UNNEST node
	idx_t index;
	//! The set of expressions
	vector<unique_ptr<Expression>> expressions;
};

//! Bound equivalent of SelectNode
class BoundSelectNode : public BoundQueryNode {
public:
	static constexpr const QueryNodeType TYPE = QueryNodeType::SELECT_NODE;

public:
	BoundSelectNode() : BoundQueryNode(QueryNodeType::SELECT_NODE) {
	}

	//! Bind information
	SelectBindState bind_state;
	//! The projection list
	vector<unique_ptr<Expression>> select_list;
	//! The FROM clause
	unique_ptr<BoundTableRef> from_table;
	//! The WHERE clause
	unique_ptr<Expression> where_clause;
	//! list of groups
	BoundGroupByNode groups;
	//! HAVING clause
	unique_ptr<Expression> having;
	//! QUALIFY clause
	unique_ptr<Expression> qualify;
	//! SAMPLE clause
	unique_ptr<SampleOptions> sample_options;

	//! The amount of columns in the final result
	idx_t column_count;
	//! The amount of bound columns in the select list
	idx_t bound_column_count = 0;

	//! Index used by the LogicalProjection
	idx_t projection_index;

	//! Group index used by the LogicalAggregate (only used if HasAggregation is true)
	idx_t group_index;
	//! Table index for the projection child of the group op
	idx_t group_projection_index;
	//! Aggregate index used by the LogicalAggregate (only used if HasAggregation is true)
	idx_t aggregate_index;
	//! Index used for GROUPINGS column references
	idx_t groupings_index;
	//! Aggregate functions to compute (only used if HasAggregation is true)
	vector<unique_ptr<Expression>> aggregates;

	//! GROUPING function calls
	vector<unsafe_vector<idx_t>> grouping_functions;

	//! Map from aggregate function to aggregate index (used to eliminate duplicate aggregates)
	expression_map_t<idx_t> aggregate_map;

	//! Window index used by the LogicalWindow (only used if HasWindow is true)
	idx_t window_index;
	//! Window functions to compute (only used if HasWindow is true)
	vector<unique_ptr<Expression>> windows;

	//! Unnest expression
	unordered_map<idx_t, BoundUnnestNode> unnests;

	//! Index of pruned node
	idx_t prune_index;
	bool need_prune = false;

public:
	idx_t GetRootIndex() override {
		return need_prune ? prune_index : projection_index;
	}
};
} // namespace duckdb

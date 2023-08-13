//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/join_order/query_graph_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/optimizer/join_order/join_node.hpp"
#include "duckdb/optimizer/join_order/join_relation.hpp"
#include "duckdb/optimizer/join_order/query_graph.hpp"
#include "duckdb/optimizer/join_order/relation_manager.hpp"
#include "duckdb/planner/column_binding.hpp"
#include "duckdb/planner/logical_operator.hpp"

#include <functional>

namespace duckdb {

struct GenerateJoinRelation {
	GenerateJoinRelation(optional_ptr<JoinRelationSet> set, unique_ptr<LogicalOperator> op_p)
	    : set(set), op(std::move(op_p)) {
	}

	optional_ptr<JoinRelationSet> set;
	unique_ptr<LogicalOperator> op;
};

//! Filter info struct that is used by the cardinality estimator to set the initial cardinality
//! but is also eventually transformed into a query edge.
struct FilterInfo {
	FilterInfo(unique_ptr<Expression> filter, JoinRelationSet &set, idx_t filter_index)
	    : filter(std::move(filter)), set(set), filter_index(filter_index) {
	}

	unique_ptr<Expression> filter;
	JoinRelationSet &set;
	idx_t filter_index;
	optional_ptr<JoinRelationSet> left_set;
	optional_ptr<JoinRelationSet> right_set;
	ColumnBinding left_binding;
	ColumnBinding right_binding;
};

//! The QueryGraphManager manages the process of extracting the reorderable and nonreorderable operations
//! from the logical plan and creating the intermediate structures needed by the plan enumerator.
//! When the plan enumerator finishes, the Query Graph Manger can then recreate the logical plan.
class QueryGraphManager {
public:
	QueryGraphManager(ClientContext &context) : relation_manager(context), context(context) {
	}

	//! manage relations and the logical operators they represent
	RelationManager relation_manager;

	//! A structure holding all the created JoinRelationSet objects
	JoinRelationSetManager set_manager;

	ClientContext &context;

	//! Extract the join relations, optimizing non-reoderable relations when encountered
	bool Build(LogicalOperator &op);

	//! Reconstruct the logical plan using the plan found by the plan enumerator
	unique_ptr<LogicalOperator> Reconstruct(unique_ptr<LogicalOperator> plan, JoinNode &node);

	//! Get a reference to the QueryGraphEdges structure that stores edges between
	//! nodes and hypernodes.
	const QueryGraphEdges &GetQueryGraphEdges() const;

	//! Get a list of the join filters in the join plan than eventually are
	//! transformed into the query graph edges
	const vector<unique_ptr<FilterInfo>> &GetFilterBindings() const;

	//! Plan enumerator may not find a full plan and therefore will need to create cross
	//! products to create edges.
	void CreateQueryGraphCrossProduct(JoinRelationSet &left, JoinRelationSet &right);

	//! after join order optimization, we perform build side probe side optimizations.
	//! (Basically we put lower expected cardinality columns on the build side, and larger
	//! tables on the probe side)
	unique_ptr<LogicalOperator> LeftRightOptimizations(unique_ptr<LogicalOperator> op);

private:
	vector<reference<LogicalOperator>> filter_operators;

	//! Filter information including the column_bindings that join filters
	//! used by the cardinality estimator to estimate distinct counts
	vector<unique_ptr<FilterInfo>> filters_and_bindings;

	QueryGraphEdges query_graph;

	void GetColumnBinding(Expression &expression, ColumnBinding &binding);

	bool ExtractBindings(Expression &expression, unordered_set<idx_t> &bindings);
	bool LeftCardLessThanRight(LogicalOperator &op);

	void CreateHyperGraphEdges();

	GenerateJoinRelation GenerateJoins(vector<unique_ptr<LogicalOperator>> &extracted_relations, JoinNode &node);

	unique_ptr<LogicalOperator> RewritePlan(unique_ptr<LogicalOperator> plan, JoinNode &node);
};

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/join_order/plan_enumerator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/optimizer/join_order/join_relation.hpp"
#include "duckdb/optimizer/join_order/cardinality_estimator.hpp"
#include "duckdb/optimizer/join_order/query_graph.hpp"
#include "duckdb/optimizer/join_order/join_node.hpp"
#include "duckdb/optimizer/join_order/cost_model.hpp"
#include "duckdb/parser/expression_map.hpp"
#include "duckdb/common/reference_map.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"

#include <functional>

namespace duckdb {

class QueryGraphManager;

class PlanEnumerator {
public:
	explicit PlanEnumerator(QueryGraphManager &query_graph_manager, CostModel &cost_model,
	                        const QueryGraphEdges &query_graph)
	    : query_graph(query_graph), query_graph_manager(query_graph_manager), cost_model(cost_model) {
	}

	//! Perform the join order solving
	void SolveJoinOrder();
	void InitLeafPlans();

	const reference_map_t<JoinRelationSet, unique_ptr<DPJoinNode>> &GetPlans() const;

private:
	//! The set of edges used in the join optimizer
	QueryGraphEdges const &query_graph;
	//! The total amount of join pairs that have been considered
	idx_t pairs = 0;
	//! Grant access to the set manager and the relation manager
	QueryGraphManager &query_graph_manager;
	//! Cost model to evaluate cost of joins
	CostModel &cost_model;
	//! A map to store the optimal join plan found for a specific JoinRelationSet*
	reference_map_t<JoinRelationSet, unique_ptr<DPJoinNode>> plans;

	unordered_set<string> join_nodes_in_full_plan;

	unique_ptr<DPJoinNode> CreateJoinTree(JoinRelationSet &set,
	                                      const vector<reference<NeighborInfo>> &possible_connections, DPJoinNode &left,
	                                      DPJoinNode &right);

	//! Emit a pair as a potential join candidate. Returns the best plan found for the (left, right) connection (either
	//! the newly created plan, or an existing plan)
	DPJoinNode &EmitPair(JoinRelationSet &left, JoinRelationSet &right, const vector<reference<NeighborInfo>> &info);
	//! Tries to emit a potential join candidate pair. Returns false if too many pairs have already been emitted,
	//! cancelling the dynamic programming step.
	bool TryEmitPair(JoinRelationSet &left, JoinRelationSet &right, const vector<reference<NeighborInfo>> &info);

	bool EnumerateCmpRecursive(JoinRelationSet &left, JoinRelationSet &right, unordered_set<idx_t> &exclusion_set);
	//! Emit a relation set node
	bool EmitCSG(JoinRelationSet &node);
	//! Enumerate the possible connected subgraphs that can be joined together in the join graph
	bool EnumerateCSGRecursive(JoinRelationSet &node, unordered_set<idx_t> &exclusion_set);
	//! Generate cross product edges inside the side
	void GenerateCrossProducts();

	//! Solve the join order exactly using dynamic programming. Returns true if it was completed successfully (i.e. did
	//! not time-out)
	bool SolveJoinOrderExactly();
	//! Solve the join order approximately using a greedy algorithm
	void SolveJoinOrderApproximately();
};

} // namespace duckdb

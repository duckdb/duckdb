//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/join_order/join_order_candidate.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/reference_map.hpp"
#include "duckdb/optimizer/join_order/cost_model.hpp"
#include "duckdb/optimizer/join_order/join_node.hpp"
#include "duckdb/optimizer/join_order/query_graph.hpp"

namespace duckdb {

class QueryGraphManager;

using JoinOrderPlanMap = reference_map_t<JoinRelationSet, unique_ptr<DPJoinNode>>;

//! Shared candidate semantics and query-lifetime connection state; search memos belong to callers.
class JoinOrderCandidateEvaluator {
public:
	JoinOrderCandidateEvaluator(QueryGraphManager &graph, CostModel &cost_model, const QueryGraphEdges &edges);

	void InitLeafPlans(JoinOrderPlanMap &plans);
	void InitInputPlan(JoinOrderPlanMap &plans);
	JoinPlanCost GetPlanCost(const DPJoinNode &node) const;
	//! Connection vectors are borrowed from the cache and invalidated when either operation below changes the graph.
	const vector<reference<NeighborInfo>> &GetConnections(JoinRelationSet &left, JoinRelationSet &right);
	//! Create an edge, invalidate cached connections, and return fresh connections for this pair.
	const vector<reference<NeighborInfo>> &CreateCrossProduct(JoinRelationSet &left, JoinRelationSet &right);
	//! Invalidate cached connections if required cross-product edges are activated.
	bool ActivateRequiredCrossProducts();

	//! Return the preferred plan for the union, or nullptr when the partition is invalid.
	optional_ptr<DPJoinNode> EmitPair(JoinOrderPlanMap &plans, JoinRelationSet &left, JoinRelationSet &right,
	                                  const vector<reference<NeighborInfo>> &connections);

private:
	unique_ptr<DPJoinNode> CreateJoinTree(JoinRelationSet &set, const vector<reference<NeighborInfo>> &connections,
	                                      DPJoinNode &left, DPJoinNode &right);
	optional_ptr<DPJoinNode> ConsiderJoinTree(JoinOrderPlanMap &plans, unique_ptr<DPJoinNode> candidate);

private:
	QueryGraphManager &query_graph_manager;
	CostModel &cost_model;
	const QueryGraphEdges &query_graph;
	double recursive_frontier_estimate = 0;
	reference_map_t<JoinRelationSet, reference_map_t<JoinRelationSet, vector<reference<NeighborInfo>>>>
	    connection_cache;
};

} // namespace duckdb

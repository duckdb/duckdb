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

	bool PreferPlan(const DPJoinNode &candidate, const JoinOrderPlanMap &candidate_plans, const DPJoinNode &incumbent,
	                const JoinOrderPlanMap &incumbent_plans) const;
	//! Conservative bound for either orientation; inputs and incumbent must be fully costed.
	bool CanImprovePair(const JoinOrderPlanMap &plans, const DPJoinNode &left, const DPJoinNode &right,
	                    const DPJoinNode &incumbent) const;

	void InitLeafPlans(JoinOrderPlanMap &plans, vector<RelationStats> relation_stats);
	void InitInputPlan(JoinOrderPlanMap &plans);
	JoinPlanCost GetPlanCost(const DPJoinNode &node) const;
	//! Borrowed vectors are invalidated by graph changes or ResetConnectionCache.
	const vector<reference<NeighborInfo>> &GetConnections(JoinRelationSet &left, JoinRelationSet &right);
	//! Create an edge, invalidate cached connections, and return fresh connections for this pair.
	const vector<reference<NeighborInfo>> &CreateCrossProduct(JoinRelationSet &left, JoinRelationSet &right);
	//! Invalidate cached connections if required cross-product edges are activated.
	bool ActivateRequiredCrossProducts();
	//! Release the previous search phase's connections after all borrowed vectors have expired.
	void ResetConnectionCache();

	//! Return the preferred plan for the union, or nullptr when the partition is invalid.
	optional_ptr<DPJoinNode> EmitPair(JoinOrderPlanMap &plans, JoinRelationSet &left, JoinRelationSet &right,
	                                  const vector<reference<NeighborInfo>> &connections);
	optional_ptr<DPJoinNode> EmitPair(JoinOrderPlanMap &plans, JoinRelationSet &set, JoinRelationSet &left,
	                                  JoinRelationSet &right, const vector<reference<NeighborInfo>> &connections);

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

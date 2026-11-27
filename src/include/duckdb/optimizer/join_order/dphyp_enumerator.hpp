//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/join_order/dphyp_enumerator.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/join_order/join_order_candidate.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"

namespace duckdb {

//! Exact enumeration retains its memo across passes, resetting traversal state before a cross-product retry.
class DPHypEnumerator {
public:
	DPHypEnumerator(QueryGraphManager &graph, const QueryGraphEdges &edges, JoinOrderCandidateEvaluator &evaluator,
	                JoinOrderPlanMap plans);

	//! Return true when traversal finishes, even if no complete plan exists; false when the pair budget is exhausted.
	bool Solve();
	//! Reset the pair budget and neighbor cache for another pass, retaining the existing memo.
	void ResetAfterGraphChange();
	const JoinOrderPlanMap &GetPlans() const;
	JoinOrderPlanMap TakePlans();

private:
	//! Return false when the exact pair limit is reached.
	bool TryEmitPair(JoinRelationSet &left, JoinRelationSet &right, const vector<reference<NeighborInfo>> &info);
	bool EmitCSG(JoinRelationSet &node);
	bool EnumerateCmpRecursive(JoinRelationSet &left, JoinRelationSet &right,
	                           unordered_set<RelationIndex> &exclusion_set);
	bool EnumerateCSGRecursive(JoinRelationSet &node, unordered_set<RelationIndex> &exclusion_set);
	const vector<reference<JoinRelationSet>> &GetAllNeighborRelationSets(vector<RelationIndex> neighbors);

private:
	QueryGraphManager &query_graph_manager;
	const QueryGraphEdges &query_graph;
	JoinOrderCandidateEvaluator &evaluator;
	JoinOrderPlanMap plans;
	idx_t pairs = 0;
	unordered_map<idx_t, vector<reference<JoinRelationSet>>> neighbor_set_cache;
};

} // namespace duckdb

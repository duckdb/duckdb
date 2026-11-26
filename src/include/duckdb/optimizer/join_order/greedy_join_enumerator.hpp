//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/join_order/greedy_join_enumerator.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/join_order/join_order_candidate.hpp"

namespace duckdb {

class GreedyJoinEnumerator {
public:
	GreedyJoinEnumerator(QueryGraphManager &graph, JoinOrderCandidateEvaluator &evaluator);

	//! The driver initializes the input plan and selects the working memo before invoking greedy search.
	//! Return true if all relations were merged into a complete plan, or false if greedy cannot complete the next
	//! merge.
	bool Solve(JoinOrderPlanMap &plans);

private:
	struct Candidate {
		idx_t left;
		idx_t right;
		JoinPlanCost cost;
	};

	idx_t AddGroup(JoinRelationSet &set);
	vector<idx_t> GetConnectedGroups(idx_t group) const;
	optional_ptr<DPJoinNode> EmitOrientation(JoinOrderPlanMap &plans, idx_t left, idx_t right);
	void EmitCandidate(JoinOrderPlanMap &plans, idx_t left, idx_t right);
	void InitializeCandidates(JoinOrderPlanMap &plans);
	bool TryCrossProduct(JoinOrderPlanMap &plans, idx_t &left, idx_t &right);
	void MergeGroups(JoinOrderPlanMap &plans, idx_t left, idx_t right);

	QueryGraphManager &query_graph_manager;
	JoinOrderCandidateEvaluator &evaluator;

	//! Group indexes are allocated in insertion order, preserving the greedy candidate tie order.
	vector<reference<JoinRelationSet>> groups;
	vector<idx_t> active_groups;
	//! The current group containing each original relation, including every member of a hyperedge endpoint.
	vector<idx_t> group_for_relation;
	vector<Candidate> candidates;
};

} // namespace duckdb

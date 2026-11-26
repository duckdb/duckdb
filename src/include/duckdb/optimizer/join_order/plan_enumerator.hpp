//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/join_order/plan_enumerator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/join_order/join_order_candidate.hpp"

namespace duckdb {

class PlanEnumerator {
public:
	explicit PlanEnumerator(QueryGraphManager &query_graph_manager, CostModel &cost_model,
	                        const QueryGraphEdges &query_graph);

	bool SolveJoinOrder();
	void InitLeafPlans();
	const JoinOrderPlanMap &GetPlans() const;

private:
	bool SolveJoinOrderApproximately();
	bool HasCompletePlan(const JoinOrderPlanMap &memo) const;
	bool PlanUsesCrossProduct(const JoinOrderPlanMap &memo, const DPJoinNode &node) const;

private:
	QueryGraphManager &query_graph_manager;
	CostModel &cost_model;
	const QueryGraphEdges &query_graph;
	JoinOrderCandidateEvaluator candidate_evaluator;
	//! Result storage also provides the initial leaves and outlives reconstruction.
	JoinOrderPlanMap plans;
};

} // namespace duckdb

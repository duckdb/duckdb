//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/join_order/linearized_plan_enumerator.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/join_order/join_order_plan.hpp"

namespace duckdb {

//! One interval trial owns its memo, initialized from immutable region inputs.
class IntervalJoinEnumerator {
public:
	IntervalJoinEnumerator(QueryGraphManager &graph, JoinOrderCandidateEvaluator &evaluator,
	                       const JoinOrderPlanMap &inputs);
	bool Solve(const vector<RelationIndex> &order);
	const JoinOrderPlanMap &GetPlans() const;

private:
	QueryGraphManager &query_graph_manager;
	JoinOrderCandidateEvaluator &evaluator;
	JoinOrderPlanMap plans;
};

//! Owns approximate-search policy and comparison of independently constructed complete plans.
class LinearizedPlanEnumerator {
public:
	LinearizedPlanEnumerator(QueryGraphManager &graph, CostModel &cost_model, JoinOrderCandidateEvaluator &evaluator);
	bool SolveApproximately(JoinOrderPlanMap &plans);

private:
	bool SolveGreedy(JoinOrderPlanMap &plans);
	unique_ptr<CapturedJoinOrderPlan> CaptureInputPlan(JoinOrderPlanMap &plans, JoinRelationSet &root);
	unique_ptr<CapturedJoinOrderPlan> CaptureGreedyPlan(JoinOrderPlanMap &plans, JoinRelationSet &root);
	void OfferPlan(JoinRelationSet &root, unique_ptr<CapturedJoinOrderPlan> candidate,
	               unique_ptr<CapturedJoinOrderPlan> &incumbent);

	QueryGraphManager &query_graph_manager;
	CostModel &cost_model;
	JoinOrderCandidateEvaluator &evaluator;
};

} // namespace duckdb

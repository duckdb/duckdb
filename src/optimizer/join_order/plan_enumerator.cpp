#include "duckdb/optimizer/join_order/plan_enumerator.hpp"

#include "duckdb/main/settings.hpp"
#include "duckdb/optimizer/join_order/dphyp_enumerator.hpp"
#include "duckdb/optimizer/join_order/linearized_plan_enumerator.hpp"
#include "duckdb/optimizer/join_order/query_graph_manager.hpp"

namespace duckdb {

PlanEnumerator::PlanEnumerator(QueryGraphManager &graph, CostModel &cost_model, const QueryGraphEdges &edges)
    : query_graph_manager(graph), cost_model(cost_model), query_graph(edges),
      candidate_evaluator(graph, cost_model, edges) {
}

const JoinOrderPlanMap &PlanEnumerator::GetPlans() const {
	return plans;
}

void PlanEnumerator::InitLeafPlans() {
	candidate_evaluator.InitLeafPlans(plans, query_graph_manager.relation_manager.GetRelationStats());
}

bool PlanEnumerator::SolveJoinOrderApproximately() {
	LinearizedPlanEnumerator linearized(query_graph_manager, cost_model, candidate_evaluator);
	return linearized.SolveApproximately(plans);
}

bool PlanEnumerator::HasCompletePlan(const JoinOrderPlanMap &memo) const {
	unordered_set<RelationIndex> bindings;
	for (idx_t relation_idx = 0; relation_idx < query_graph_manager.relation_manager.NumRelations(); relation_idx++) {
		bindings.emplace(relation_idx);
	}
	auto &total_relation = query_graph_manager.set_manager.GetJoinRelation(bindings);
	return memo.find(total_relation) != memo.end();
}

bool PlanEnumerator::PlanUsesCrossProduct(const JoinOrderPlanMap &memo, const DPJoinNode &node) const {
	if (node.is_leaf) {
		return false;
	}
	if (node.generated_cross_product ||
	    (node.join_operator && node.join_operator->type == JoinOrderOperatorType::CROSS_PRODUCT)) {
		return true;
	}
	auto left = memo.find(node.left_set);
	auto right = memo.find(node.right_set);
	D_ASSERT(left != memo.end() && right != memo.end());
	return PlanUsesCrossProduct(memo, *left->second) || PlanUsesCrossProduct(memo, *right->second);
}

bool PlanEnumerator::SolveJoinOrder() {
	bool force_no_cross_product = Settings::Get<DebugForceNoCrossProductSetting>(query_graph_manager.context);
	auto swap_to_approximate_threshold =
	    Settings::Get<ApproximateJoinOrderThresholdSetting>(query_graph_manager.context);

	// first try to solve the join order exactly
	bool solved;
	if (query_graph_manager.relation_manager.NumRelations() >= swap_to_approximate_threshold) {
		solved = SolveJoinOrderApproximately();
	} else {
		bool completed_exactly;
		{
			DPHypEnumerator exact(query_graph_manager, query_graph, candidate_evaluator, std::move(plans));
			completed_exactly = exact.Solve();
			if (completed_exactly && !HasCompletePlan(exact.GetPlans()) &&
			    candidate_evaluator.ActivateRequiredCrossProducts()) {
				exact.ResetAfterGraphChange();
				completed_exactly = exact.Solve();
			}
			plans = exact.TakePlans();
		}
		if (!completed_exactly || !HasCompletePlan(plans)) {
			// Exact enumeration either reached its pair budget or could not connect the graph.
			solved = SolveJoinOrderApproximately();
		} else {
			solved = true;
		}
	}

	// now the optimal join path should have been found
	// get it from the node
	unordered_set<RelationIndex> bindings;
	for (idx_t i = 0; i < query_graph_manager.relation_manager.NumRelations(); i++) {
		bindings.emplace(i);
	}
	auto &total_relation = query_graph_manager.set_manager.GetJoinRelation(bindings);
	auto final_plan = plans.find(total_relation);
	if (!solved) {
		if (force_no_cross_product && query_graph_manager.RequiresCrossProduct()) {
			throw InvalidInputException(
			    "Query requires a cross-product, but 'force_no_cross_product' PRAGMA is enabled");
		}
		return false;
	}
	if (final_plan == plans.end()) {
		if (force_no_cross_product) {
			throw InvalidInputException(
			    "Query requires a cross-product, but 'force_no_cross_product' PRAGMA is enabled");
		}
		return false;
	}
	if (force_no_cross_product && PlanUsesCrossProduct(plans, *final_plan->second)) {
		throw InvalidInputException("Query requires a cross-product, but 'force_no_cross_product' PRAGMA is enabled");
	}
	return true;
}

} // namespace duckdb

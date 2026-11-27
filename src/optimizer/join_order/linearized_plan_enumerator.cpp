#include "duckdb/optimizer/join_order/linearized_plan_enumerator.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/optimizer/join_order/greedy_join_enumerator.hpp"
#include "duckdb/optimizer/join_order/join_order_linearizer.hpp"
#include "duckdb/optimizer/join_order/join_order_precedence.hpp"
#include "duckdb/optimizer/join_order/query_graph_manager.hpp"

namespace duckdb {

IntervalJoinEnumerator::IntervalJoinEnumerator(QueryGraphManager &graph, JoinOrderCandidateEvaluator &evaluator,
                                               const JoinOrderPlanMap &inputs)
    : query_graph_manager(graph), evaluator(evaluator), plans(CopyJoinOrderLeafPlans(inputs)) {
}

const JoinOrderPlanMap &IntervalJoinEnumerator::GetPlans() const {
	return plans;
}

LinearizedPlanEnumerator::LinearizedPlanEnumerator(QueryGraphManager &graph, CostModel &cost_model,
                                                   JoinOrderCandidateEvaluator &evaluator)
    : query_graph_manager(graph), cost_model(cost_model), evaluator(evaluator) {
}

bool LinearizedPlanEnumerator::SolveGreedy(JoinOrderPlanMap &plans) {
	evaluator.InitInputPlan(plans);
	GreedyJoinEnumerator greedy(query_graph_manager, evaluator);
	return greedy.Solve(plans);
}

unique_ptr<CapturedJoinOrderPlan> LinearizedPlanEnumerator::CaptureInputPlan(JoinOrderPlanMap &plans,
                                                                             JoinRelationSet &root) {
	evaluator.InitInputPlan(plans);
	if (plans.find(root) == plans.end()) {
		return nullptr;
	}
	return CapturedJoinOrderPlan::Capture(plans, root, cost_model);
}

unique_ptr<CapturedJoinOrderPlan> LinearizedPlanEnumerator::CaptureGreedyPlan(JoinOrderPlanMap &plans,
                                                                              JoinRelationSet &root) {
	GreedyJoinEnumerator greedy(query_graph_manager, evaluator);
	if (!greedy.Solve(plans)) {
		return nullptr;
	}
	return CapturedJoinOrderPlan::Capture(plans, root, cost_model);
}

void LinearizedPlanEnumerator::OfferPlan(JoinRelationSet &root, unique_ptr<CapturedJoinOrderPlan> candidate,
                                         unique_ptr<CapturedJoinOrderPlan> &incumbent) {
	if (&candidate->GetRoot() != &root || (incumbent && &incumbent->GetRoot() != &root)) {
		throw InternalException("Join-order incumbent root does not match the search region");
	}
	if (!incumbent || evaluator.PreferPlan(candidate->GetRootNode(), candidate->GetPlans(), incumbent->GetRootNode(),
	                                       incumbent->GetPlans())) {
		incumbent = std::move(candidate);
	}
}

bool IntervalJoinEnumerator::Solve(const vector<RelationIndex> &order) {
	auto count = order.size();
	D_ASSERT(count > 0);
	// Each trial starts with the same immutable leaves. In particular, a winner from another order may not
	// replace an interval's child after its parent has been costed.
	vector<vector<optional_ptr<JoinRelationSet>>> intervals(count);
	for (idx_t start = 0; start < count; start++) {
		intervals[start].resize(count - start);
		intervals[start][0] = query_graph_manager.set_manager.GetJoinRelation(order[start]);
	}
	for (idx_t length = 2; length <= count; length++) {
		for (idx_t start = 0; start + length <= count; start++) {
			query_graph_manager.context.InterruptCheck();
			auto &set =
			    query_graph_manager.set_manager.Union(*intervals[start][length - 2], *intervals[start + length - 1][0]);
			intervals[start][length - 1] = set;
		}
	}
	for (idx_t length = 2; length <= count; length++) {
		for (idx_t start = 0; start + length <= count; start++) {
			auto &set = *intervals[start][length - 1];
			for (idx_t split = 1; split < length; split++) {
				query_graph_manager.context.InterruptCheck();
				auto &left = *intervals[start][split - 1];
				auto &right = *intervals[start + split][length - split - 1];
				auto left_plan = plans.find(left);
				auto right_plan = plans.find(right);
				if (left_plan == plans.end() || right_plan == plans.end()) {
					continue;
				}
				auto incumbent = plans.find(set);
				if (incumbent != plans.end() &&
				    !evaluator.CanImprovePair(plans, *left_plan->second, *right_plan->second, *incumbent->second)) {
					continue;
				}
				// A trial visits each ordered pair once; do not retain a cubic connection cache.
				auto &edges = query_graph_manager.GetQueryGraphEdges();
				evaluator.EmitPair(plans, set, left, right, edges.GetConnections(left, right));
				evaluator.EmitPair(plans, set, right, left, edges.GetConnections(right, left));
			}
		}
	}
	return plans.find(*intervals[0][count - 1]) != plans.end();
}

bool LinearizedPlanEnumerator::SolveApproximately(JoinOrderPlanMap &plans) {
	// Larger regions need subtree refinement rather than a full cubic pass.
	if (query_graph_manager.relation_manager.NumRelations() > 32) {
		return SolveGreedy(plans);
	}
	JoinOrderLinearizer linearizer(query_graph_manager, cost_model.GetCardinalityEstimator());
	auto ordinary = linearizer.BuildTree();
	unordered_set<RelationIndex> bindings;
	for (idx_t i = 0; i < query_graph_manager.relation_manager.NumRelations(); i++) {
		bindings.emplace(i);
	}
	auto &root = query_graph_manager.set_manager.GetJoinRelation(bindings);
	unique_ptr<CapturedJoinOrderPlan> incumbent;
	if (plans.find(root) != plans.end()) {
		OfferPlan(root, CapturedJoinOrderPlan::Capture(plans, root, cost_model), incumbent);
	}
	// Partial exact nodes may have stale ancestor costs. Each new search starts from immutable leaves.
	plans = CopyJoinOrderLeafPlans(plans);
	evaluator.ResetConnectionCache();
	unique_ptr<CapturedJoinOrderPlan> input_plan, greedy_plan;
	bool greedy_ran = false;
	vector<RelationIndex> order;
	if (ordinary) {
		order = linearizer.GetBestOrder();
	} else {
		input_plan = CaptureInputPlan(plans, root);
		optional_ptr<CapturedJoinOrderPlan> witness = input_plan ? input_plan.get() : incumbent.get();
		if (!witness) {
			// Some input trees need cross-product activation before a complete witness is available.
			greedy_ran = true;
			greedy_plan = CaptureGreedyPlan(plans, root);
			plans = CopyJoinOrderLeafPlans(plans);
			input_plan = CaptureInputPlan(plans, root);
			witness = input_plan ? input_plan.get() : greedy_plan.get();
		}
		if (!witness) {
			return false;
		}
		JoinOrderPrecedence precedence(query_graph_manager, cost_model.GetCardinalityEstimator(), evaluator, *witness);
		order = precedence.GetBestOrder();
		if (order.empty()) {
			order = witness->GetLeafOrder();
		}
	}
	{
		IntervalJoinEnumerator trial(query_graph_manager, evaluator, plans);
		if (trial.Solve(order)) {
			OfferPlan(root, CapturedJoinOrderPlan::Capture(trial.GetPlans(), root, cost_model), incumbent);
		}
	}
	if (ordinary) {
		input_plan = CaptureInputPlan(plans, root);
	}
	if (input_plan) {
		OfferPlan(root, std::move(input_plan), incumbent);
	}
	// Greedy explores different bushy plans. Compare complete candidates without mixing their memos.
	if (!greedy_ran) {
		greedy_plan = CaptureGreedyPlan(plans, root);
	}
	if (greedy_plan) {
		OfferPlan(root, std::move(greedy_plan), incumbent);
	}
	if (!incumbent) {
		return false;
	}
	plans = incumbent->TakePlans();
	return true;
}

} // namespace duckdb

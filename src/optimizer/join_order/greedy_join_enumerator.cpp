#include "duckdb/optimizer/join_order/greedy_join_enumerator.hpp"

#include "duckdb/optimizer/join_order/query_graph_manager.hpp"

namespace duckdb {

GreedyJoinEnumerator::GreedyJoinEnumerator(QueryGraphManager &graph, JoinOrderCandidateEvaluator &evaluator)
    : query_graph_manager(graph), evaluator(evaluator) {
}

bool GreedyJoinEnumerator::Solve(JoinOrderPlanMap &plans) {
	// at this point, we exited the dynamic programming but did not compute the final join order because it took too
	// long instead, we use a greedy heuristic to obtain a join ordering now we use Greedy Operator Ordering to
	// construct the result tree first we start out with all the base relations (the to-be-joined relations)
	vector<reference<JoinRelationSet>> join_relations; // T in the paper
	for (idx_t i = 0; i < query_graph_manager.relation_manager.NumRelations(); i++) {
		join_relations.push_back(query_graph_manager.set_manager.GetJoinRelation(RelationIndex(i)));
	}

	// in every step of the algorithm, we greedily pick the join between the to-be-joined relations that has the
	// smallest cost. Pair costs are deterministic while the query graph is unchanged, so we evaluate each pair once
	// and keep the viable candidates in a list, instead of re-emitting every pair in every step
	struct GreedyCandidate {
		optional_ptr<JoinRelationSet> left;
		optional_ptr<JoinRelationSet> right;
		JoinPlanCost cost;
	};
	vector<GreedyCandidate> candidates;
	auto EmitOrientation = [&](JoinRelationSet &left, JoinRelationSet &right) -> optional_ptr<DPJoinNode> {
		auto &connection = evaluator.GetConnections(left, right);
		if (connection.empty()) {
			return nullptr;
		}
		return evaluator.EmitPair(plans, left, right, connection);
	};
	auto EmitCandidate = [&](JoinRelationSet &left, JoinRelationSet &right) {
		// non-commutative joins (e.g. LEFT, SEMI, ANTI) are only valid in one orientation
		auto node = EmitOrientation(left, right);
		if (!node) {
			node = EmitOrientation(right, left);
		}
		if (node) {
			candidates.push_back({&left, &right, evaluator.GetPlanCost(*node)});
		}
	};
	auto InitializeCandidates = [&]() {
		candidates.clear();
		for (idx_t i = 0; i < join_relations.size(); i++) {
			for (idx_t j = i + 1; j < join_relations.size(); j++) {
				EmitCandidate(join_relations[i], join_relations[j]);
			}
		}
	};
	InitializeCandidates();

	while (join_relations.size() > 1) {
		optional_ptr<JoinRelationSet> best_left;
		optional_ptr<JoinRelationSet> best_right;
		JoinPlanCost best_cost {NumericLimits<double>::Maximum(), NumericLimits<double>::Maximum()};
		for (auto &candidate : candidates) {
			if (!best_left || candidate.cost < best_cost) {
				best_cost = candidate.cost;
				best_left = candidate.left;
				best_right = candidate.right;
			}
		}
		if (!best_left) {
			if (evaluator.ActivateRequiredCrossProducts()) {
				// the query graph gained edges, which invalidates the evaluated candidates
				InitializeCandidates();
				continue;
			}
			// Optimizer-introduced cross products are valid only inside an inner-join companion set (Section 6.2).
			for (idx_t i = 0; i < join_relations.size(); i++) {
				auto &left = join_relations[i].get();
				for (idx_t j = i + 1; j < join_relations.size(); j++) {
					auto &right = join_relations[j].get();
					if (!query_graph_manager.CanCreateCrossProduct(left, right)) {
						continue;
					}
					auto left_plan = plans.find(left);
					auto right_plan = plans.find(right);
					D_ASSERT(left_plan != plans.end() && right_plan != plans.end());
					auto left_cost = evaluator.GetPlanCost(*left_plan->second);
					auto right_cost = evaluator.GetPlanCost(*right_plan->second);
					JoinPlanCost cost {left_cost.work + right_cost.work,
					                   MaxValue(left_cost.memory_excess, right_cost.memory_excess)};
					if (!best_left || cost < best_cost) {
						best_cost = cost;
						best_left = left;
						best_right = right;
					}
				}
			}
			if (!best_left) {
				return false;
			}
			// create a cross product edge (i.e. edge with empty filter) between these two sets in the query graph
			auto &connections = evaluator.CreateCrossProduct(*best_left, *best_right);
			// the new edge only connects this pair, which is merged below, so the other candidates remain valid
			// now emit the pair and continue with the algorithm
			D_ASSERT(!connections.empty());

			auto cross_product = evaluator.EmitPair(plans, *best_left, *best_right, connections);
			if (!cross_product) {
				return false;
			}
		}
		// now update the to-be-checked pairs: remove left and right, and add the combination
		auto &new_set = query_graph_manager.set_manager.Union(*best_left, *best_right);
		for (idx_t i = join_relations.size(); i > 0; i--) {
			auto &relation = join_relations[i - 1].get();
			if (&relation == best_left.get() || &relation == best_right.get()) {
				join_relations.erase(join_relations.begin() + NumericCast<int64_t>(i - 1));
			}
		}
		idx_t kept_candidates = 0;
		for (idx_t candidate_idx = 0; candidate_idx < candidates.size(); candidate_idx++) {
			auto &candidate = candidates[candidate_idx];
			if (candidate.left == best_left || candidate.left == best_right || candidate.right == best_left ||
			    candidate.right == best_right) {
				continue;
			}
			candidates[kept_candidates++] = candidate;
		}
		candidates.resize(kept_candidates);
		for (auto &relation : join_relations) {
			EmitCandidate(relation, new_set);
		}
		join_relations.push_back(new_set);
	}
	return true;
}

} // namespace duckdb

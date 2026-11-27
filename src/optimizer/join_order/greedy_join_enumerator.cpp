#include "duckdb/optimizer/join_order/greedy_join_enumerator.hpp"

#include "duckdb/optimizer/join_order/query_graph_manager.hpp"

#include <algorithm>

namespace duckdb {

GreedyJoinEnumerator::GreedyJoinEnumerator(QueryGraphManager &graph, JoinOrderCandidateEvaluator &evaluator)
    : query_graph_manager(graph), evaluator(evaluator) {
}

idx_t GreedyJoinEnumerator::AddGroup(JoinRelationSet &set) {
	auto group = groups.size();
	groups.push_back(set);
	active_groups.push_back(group);
	for (idx_t i = 0; i < set.count; i++) {
		group_for_relation[set.relations[i].index] = group;
	}
	return group;
}

vector<idx_t> GreedyJoinEnumerator::GetConnectedGroups(idx_t group) const {
	unordered_set<idx_t> connected_groups;
	auto &set = groups[group].get();
	query_graph_manager.GetQueryGraphEdges().EnumerateNeighbors(set, [&](NeighborInfo &connection) {
		D_ASSERT(connection.neighbor && !connection.neighbor->Empty());
		auto &neighbor = *connection.neighbor;
		auto other_group = group_for_relation[neighbor.relations[0].index];
		if (other_group != group && JoinRelationSet::IsSubset(groups[other_group].get(), neighbor)) {
			connected_groups.insert(other_group);
		}
		return false;
	});
	vector<idx_t> result(connected_groups.begin(), connected_groups.end());
	// The graph traversal order is unspecified. Keep the same order as the active-group list.
	std::sort(result.begin(), result.end());
	return result;
}

optional_ptr<DPJoinNode> GreedyJoinEnumerator::EmitOrientation(JoinOrderPlanMap &plans, idx_t left, idx_t right) {
	auto &connections = evaluator.GetConnections(groups[left], groups[right]);
	if (connections.empty()) {
		return nullptr;
	}
	return evaluator.EmitPair(plans, groups[left], groups[right], connections);
}

void GreedyJoinEnumerator::EmitCandidate(JoinOrderPlanMap &plans, idx_t left, idx_t right) {
	// Non-commutative joins (e.g. LEFT, SEMI, ANTI) are only valid in one orientation.
	auto node = EmitOrientation(plans, left, right);
	if (!node) {
		node = EmitOrientation(plans, right, left);
	}
	if (node) {
		candidates.push_back({left, right, evaluator.GetPlanCost(*node)});
	}
}

void GreedyJoinEnumerator::InitializeCandidates(JoinOrderPlanMap &plans) {
	candidates.clear();
	for (auto group : active_groups) {
		for (auto other_group : GetConnectedGroups(group)) {
			if (other_group > group) {
				EmitCandidate(plans, group, other_group);
			}
		}
	}
}

bool GreedyJoinEnumerator::TryCrossProduct(JoinOrderPlanMap &plans, idx_t &left, idx_t &right) {
	optional_idx best_left;
	optional_idx best_right;
	JoinPlanCost best_cost {NumericLimits<double>::Maximum(), NumericLimits<double>::Maximum()};
	// Optimizer-introduced cross products are valid only inside an inner-join companion set (Section 6.2).
	for (idx_t i = 0; i < active_groups.size(); i++) {
		auto left_group = active_groups[i];
		auto &left_set = groups[left_group].get();
		for (idx_t j = i + 1; j < active_groups.size(); j++) {
			auto right_group = active_groups[j];
			auto &right_set = groups[right_group].get();
			if (!query_graph_manager.CanCreateCrossProduct(left_set, right_set)) {
				continue;
			}
			auto left_plan = plans.find(left_set);
			auto right_plan = plans.find(right_set);
			D_ASSERT(left_plan != plans.end() && right_plan != plans.end());
			auto left_cost = evaluator.GetPlanCost(*left_plan->second);
			auto right_cost = evaluator.GetPlanCost(*right_plan->second);
			JoinPlanCost cost {left_cost.work + right_cost.work,
			                   MaxValue(left_cost.memory_excess, right_cost.memory_excess)};
			if (!best_left.IsValid() || cost < best_cost) {
				best_cost = cost;
				best_left = optional_idx(left_group);
				best_right = optional_idx(right_group);
			}
		}
	}
	if (!best_left.IsValid()) {
		return false;
	}
	left = best_left.GetIndex();
	right = best_right.GetIndex();
	// This new edge only connects the pair merged next, so the other candidates remain valid.
	auto &connections = evaluator.CreateCrossProduct(groups[left], groups[right]);
	D_ASSERT(!connections.empty());
	return evaluator.EmitPair(plans, groups[left], groups[right], connections) != nullptr;
}

void GreedyJoinEnumerator::MergeGroups(JoinOrderPlanMap &plans, idx_t left, idx_t right) {
	auto &new_set = query_graph_manager.set_manager.Union(groups[left], groups[right]);
	auto removed = [&](idx_t group) {
		return group == left || group == right;
	};
	active_groups.erase(std::remove_if(active_groups.begin(), active_groups.end(), removed), active_groups.end());
	idx_t kept_candidates = 0;
	for (idx_t i = 0; i < candidates.size(); i++) {
		auto &candidate = candidates[i];
		if (!removed(candidate.left) && !removed(candidate.right)) {
			candidates[kept_candidates++] = candidate;
		}
	}
	candidates.resize(kept_candidates);
	auto new_group = AddGroup(new_set);
	// Graph construction inserts both edge orientations. Visiting the new group also finds hyperedges with an
	// endpoint completed by this merge; full-endpoint containment is checked before evaluation.
	for (auto other_group : GetConnectedGroups(new_group)) {
		EmitCandidate(plans, other_group, new_group);
	}
}

bool GreedyJoinEnumerator::Solve(JoinOrderPlanMap &plans) {
	groups.clear();
	active_groups.clear();
	candidates.clear();
	group_for_relation.resize(query_graph_manager.relation_manager.NumRelations());
	for (idx_t i = 0; i < group_for_relation.size(); i++) {
		AddGroup(query_graph_manager.set_manager.GetJoinRelation(RelationIndex(i)));
	}
	InitializeCandidates(plans);

	// Pair costs are deterministic while the graph is unchanged. Keep viable candidates until an input is merged.
	while (active_groups.size() > 1) {
		optional_idx best;
		for (idx_t i = 0; i < candidates.size(); i++) {
			if (!best.IsValid() || candidates[i].cost < candidates[best.GetIndex()].cost) {
				best = optional_idx(i);
			}
		}
		idx_t left;
		idx_t right;
		if (best.IsValid()) {
			left = candidates[best.GetIndex()].left;
			right = candidates[best.GetIndex()].right;
		} else {
			if (evaluator.ActivateRequiredCrossProducts()) {
				// The graph gained edges, invalidating evaluated candidates.
				InitializeCandidates(plans);
				continue;
			}
			if (!TryCrossProduct(plans, left, right)) {
				return false;
			}
		}
		MergeGroups(plans, left, right);
	}
	return true;
}

} // namespace duckdb

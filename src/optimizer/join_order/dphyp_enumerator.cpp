#include "duckdb/optimizer/join_order/dphyp_enumerator.hpp"

#include "duckdb/optimizer/join_order/query_graph_manager.hpp"

#include <algorithm>
#include <cmath>
#include <functional>

namespace duckdb {

DPHypEnumerator::DPHypEnumerator(QueryGraphManager &graph, const QueryGraphEdges &edges,
                                 JoinOrderCandidateEvaluator &evaluator, JoinOrderPlanMap plans)
    : query_graph_manager(graph), query_graph(edges), evaluator(evaluator), plans(std::move(plans)) {
}

const JoinOrderPlanMap &DPHypEnumerator::GetPlans() const {
	return plans;
}

JoinOrderPlanMap DPHypEnumerator::TakePlans() {
	return std::move(plans);
}

void DPHypEnumerator::ResetAfterGraphChange() {
	pairs = 0;
	neighbor_set_cache.clear();
}

static vector<unordered_set<RelationIndex>> AddSuperSets(const vector<unordered_set<RelationIndex>> &current,
                                                         const vector<RelationIndex> &all_neighbors) {
	vector<unordered_set<RelationIndex>> ret;

	for (const auto &neighbor_set : current) {
		auto max_val = std::max_element(neighbor_set.begin(), neighbor_set.end());
		for (const auto &neighbor : all_neighbors) {
			if (*max_val >= neighbor) {
				continue;
			}
			if (neighbor_set.count(neighbor) == 0) {
				unordered_set<RelationIndex> new_set;
				for (auto &n : neighbor_set) {
					new_set.insert(n);
				}
				new_set.insert(neighbor);
				ret.push_back(new_set);
			}
		}
	}

	return ret;
}

//! Update the exclusion set with all entries in the subgraph
static void UpdateExclusionSet(optional_ptr<JoinRelationSet> node, unordered_set<RelationIndex> &exclusion_set) {
	for (idx_t i = 0; i < node->count; i++) {
		exclusion_set.insert(node->relations[i]);
	}
}

// works by first creating all sets with cardinality 1
// then iterates over each previously created group of subsets and will only add a neighbor if the neighbor
// is greater than all relations in the set.
static vector<unordered_set<RelationIndex>> GetAllNeighborSets(vector<RelationIndex> neighbors) {
	vector<unordered_set<RelationIndex>> ret;
	sort(neighbors.begin(), neighbors.end());
	vector<unordered_set<RelationIndex>> added;
	for (auto &neighbor : neighbors) {
		added.push_back(unordered_set<RelationIndex>({neighbor}));
		ret.push_back(unordered_set<RelationIndex>({neighbor}));
	}
	do {
		added = AddSuperSets(added, neighbors);
		for (auto &d : added) {
			ret.push_back(d);
		}
	} while (!added.empty());
#if DEBUG
	// drive by test to make sure we have an accurate amount of
	// subsets, and that each neighbor is in a correct amount
	// of those subsets.
	D_ASSERT(ret.size() == static_cast<size_t>(std::pow(2, neighbors.size())) - 1);
	for (auto &n : neighbors) {
		idx_t count = 0;
		for (auto &set : ret) {
			if (set.count(n) >= 1) {
				count += 1;
			}
		}
		D_ASSERT(count == static_cast<size_t>(std::pow(2, neighbors.size() - 1)));
	}
#endif
	return ret;
}

static idx_t GetNeighborSetCacheKey(const vector<RelationIndex> &neighbors) {
	idx_t key = 0;
	for (auto &neighbor : neighbors) {
		D_ASSERT(neighbor.index < sizeof(idx_t) * 8);
		key |= idx_t(1) << neighbor.index;
	}
	return key;
}

const vector<reference<JoinRelationSet>> &DPHypEnumerator::GetAllNeighborRelationSets(vector<RelationIndex> neighbors) {
	auto key = GetNeighborSetCacheKey(neighbors);
	auto entry = neighbor_set_cache.find(key);
	if (entry != neighbor_set_cache.end()) {
		return entry->second;
	}

	auto all_subset = GetAllNeighborSets(std::move(neighbors));
	vector<reference<JoinRelationSet>> relation_sets;
	relation_sets.reserve(all_subset.size());
	for (const auto &rel_set : all_subset) {
		relation_sets.push_back(query_graph_manager.set_manager.GetJoinRelation(rel_set));
	}
	auto inserted = neighbor_set_cache.insert(make_pair(key, std::move(relation_sets)));
	return inserted.first->second;
}

bool DPHypEnumerator::TryEmitPair(JoinRelationSet &left, JoinRelationSet &right,
                                  const vector<reference<NeighborInfo>> &info) {
	pairs++;
	// If a full plan is created, it's possible a node in the plan gets updated. When this happens, make sure you keep
	// emitting pairs until you emit another final plan. Another final plan is guaranteed to be produced because of
	// our symmetry guarantees.
	if (pairs >= 10000) {
		// when the amount of pairs gets too large we exit the dynamic programming and resort to a greedy algorithm
		// FIXME: simple heuristic currently
		// at 10K pairs stop searching exactly and switch to heuristic
		return false;
	}
	evaluator.EmitPair(plans, left, right, info);
	return true;
}

bool DPHypEnumerator::EmitCSG(JoinRelationSet &node) {
	if (node.count == query_graph_manager.relation_manager.NumRelations()) {
		return true;
	}
	// create the exclusion set as everything inside the subgraph AND anything with members BELOW it
	unordered_set<RelationIndex> exclusion_set;
	for (idx_t i = 0; i < node.relations[0].index; i++) {
		exclusion_set.emplace(i);
	}
	UpdateExclusionSet(&node, exclusion_set);
	// find the neighbors given this exclusion set
	auto neighbors = query_graph.GetNeighbors(node, exclusion_set);
	if (neighbors.empty()) {
		return true;
	}

	//! Neighbors should be reversed when iterating over them.
	std::sort(neighbors.begin(), neighbors.end(), std::greater<RelationIndex>());
	for (idx_t i = 0; i < neighbors.size() - 1; i++) {
		D_ASSERT(neighbors[i] > neighbors[i + 1]);
	}

	// Dphyp paper missing this.
	// Because we are traversing in reverse order, we need to add neighbors whose number is smaller than the current
	// node to exclusion_set
	// This avoids duplicated enumeration
	unordered_set<RelationIndex> new_exclusion_set = exclusion_set;
	for (idx_t i = 0; i < neighbors.size(); ++i) {
		D_ASSERT(new_exclusion_set.find(neighbors[i]) == new_exclusion_set.end());
		new_exclusion_set.insert(neighbors[i]);
	}

	for (auto neighbor : neighbors) {
		// since the GetNeighbors only returns the smallest element in a list, the entry might not be connected to
		// (only!) this neighbor,  hence we have to do a connectedness check before we can emit it
		auto &neighbor_relation = query_graph_manager.set_manager.GetJoinRelation(neighbor);
		auto &connections = evaluator.GetConnections(node, neighbor_relation);
		if (!connections.empty()) {
			if (!TryEmitPair(node, neighbor_relation, connections)) {
				return false;
			}
		}

		if (!EnumerateCmpRecursive(node, neighbor_relation, new_exclusion_set)) {
			return false;
		}

		new_exclusion_set.erase(neighbor);
	}
	return true;
}

bool DPHypEnumerator::EnumerateCmpRecursive(JoinRelationSet &left, JoinRelationSet &right,
                                            unordered_set<RelationIndex> &exclusion_set) {
	// get the neighbors of the second relation under the exclusion set
	auto neighbors = query_graph.GetNeighbors(right, exclusion_set);
	if (neighbors.empty()) {
		return true;
	}

	auto &all_subset = GetAllNeighborRelationSets(neighbors);
	vector<reference<JoinRelationSet>> union_sets;
	union_sets.reserve(all_subset.size());
	for (auto &neighbor : all_subset) {
		// emit the combinations of this node and its neighbors
		auto &combined_set = query_graph_manager.set_manager.Union(right, neighbor.get());
		// If combined_set.count == right.count, This means we found a neighbor that has been present before
		// This means we didn't set exclusion_set correctly.
		D_ASSERT(combined_set.count > right.count);
		if (plans.find(combined_set) != plans.end()) {
			auto &connections = evaluator.GetConnections(left, combined_set);
			if (!connections.empty()) {
				if (!TryEmitPair(left, combined_set, connections)) {
					return false;
				}
			}
		}
		union_sets.push_back(combined_set);
	}

	unordered_set<RelationIndex> new_exclusion_set = exclusion_set;
	for (const auto &neighbor : neighbors) {
		new_exclusion_set.insert(neighbor);
	}

	// recursively enumerate the sets
	for (idx_t i = 0; i < union_sets.size(); i++) {
		// updated the set of excluded entries with this neighbor
		if (!EnumerateCmpRecursive(left, union_sets[i], new_exclusion_set)) {
			return false;
		}
	}
	return true;
}

bool DPHypEnumerator::EnumerateCSGRecursive(JoinRelationSet &node, unordered_set<RelationIndex> &exclusion_set) {
	// find neighbors of S under the exclusion set
	auto neighbors = query_graph.GetNeighbors(node, exclusion_set);
	if (neighbors.empty()) {
		return true;
	}

	auto &all_subset = GetAllNeighborRelationSets(neighbors);
	vector<reference<JoinRelationSet>> union_sets;
	union_sets.reserve(all_subset.size());
	for (auto &neighbor : all_subset) {
		// emit the combinations of this node and its neighbors
		auto &new_set = query_graph_manager.set_manager.Union(node, neighbor.get());
		D_ASSERT(new_set.count > node.count);
		if (plans.find(new_set) != plans.end()) {
			if (!EmitCSG(new_set)) {
				return false;
			}
		}
		union_sets.push_back(new_set);
	}

	unordered_set<RelationIndex> new_exclusion_set = exclusion_set;
	for (const auto &neighbor : neighbors) {
		new_exclusion_set.insert(neighbor);
	}

	// recursively enumerate the sets
	for (idx_t i = 0; i < union_sets.size(); i++) {
		// updated the set of excluded entries with this neighbor
		if (!EnumerateCSGRecursive(union_sets[i], new_exclusion_set)) {
			return false;
		}
	}
	return true;
}

// "Dynamic Programming Strikes Back" by Guido Moerkotte and Thomas Neumann.
// https://db.in.tum.de/teaching/ws1415/queryopt/chapter3.pdf?lang=de
bool DPHypEnumerator::Solve() {
	// now we perform the actual dynamic programming to compute the final result
	// we enumerate over all the possible pairs in the neighborhood
	for (idx_t i = query_graph_manager.relation_manager.NumRelations(); i > 0; i--) {
		// for every node in the set, we consider it as the start node once
		auto &start_node = query_graph_manager.set_manager.GetJoinRelation(RelationIndex(i - 1));
		// emit the start node
		if (!EmitCSG(start_node)) {
			return false;
		}
		// initialize the set of exclusion_set as all the nodes with a number below this
		unordered_set<RelationIndex> exclusion_set;
		for (idx_t j = 0; j < i; j++) {
			exclusion_set.emplace(j);
		}
		// then we recursively search for neighbors that do not belong to the banned entries
		if (!EnumerateCSGRecursive(start_node, exclusion_set)) {
			return false;
		}
	}
	return true;
}

} // namespace duckdb

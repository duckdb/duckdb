#include "duckdb/optimizer/join_order/plan_enumerator.hpp"
#include "duckdb/optimizer/join_order/query_graph_manager.hpp"

namespace duckdb {

class JoinNode;

bool PlanEnumerator::NodeInFullPlan(JoinNode &node) {
	return join_nodes_in_full_plan.find(node.set->ToString()) != join_nodes_in_full_plan.end();
}

void PlanEnumerator::UpdateJoinNodesInFullPlan(JoinNode &node) {
	if (node.set->count == query_graph_manager.relation_manager.NumRelations()) {
		join_nodes_in_full_plan.clear();
	}
	if (node.set->count < query_graph_manager.relation_manager.NumRelations()) {
		join_nodes_in_full_plan.insert(node.set->ToString());
	}
	if (node.left) {
		UpdateJoinNodesInFullPlan(*node.left);
	}
	if (node.right) {
		UpdateJoinNodesInFullPlan(*node.right);
	}
}

static vector<unordered_set<idx_t>> AddSuperSets(const vector<unordered_set<idx_t>> &current,
                                                 const vector<idx_t> &all_neighbors) {
	vector<unordered_set<idx_t>> ret;

	for (const auto &neighbor_set : current) {
		auto max_val = std::max_element(neighbor_set.begin(), neighbor_set.end());
		for (const auto &neighbor : all_neighbors) {
			if (*max_val >= neighbor) {
				continue;
			}
			if (neighbor_set.count(neighbor) == 0) {
				unordered_set<idx_t> new_set;
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
static void UpdateExclusionSet(optional_ptr<JoinRelationSet> node, unordered_set<idx_t> &exclusion_set) {
	for (idx_t i = 0; i < node->count; i++) {
		exclusion_set.insert(node->relations[i]);
	}
}

// works by first creating all sets with cardinality 1
// then iterates over each previously created group of subsets and will only add a neighbor if the neighbor
// is greater than all relations in the set.
static vector<unordered_set<idx_t>> GetAllNeighborSets(vector<idx_t> neighbors) {
	vector<unordered_set<idx_t>> ret;
	sort(neighbors.begin(), neighbors.end());
	vector<unordered_set<idx_t>> added;
	for (auto &neighbor : neighbors) {
		added.push_back(unordered_set<idx_t>({neighbor}));
		ret.push_back(unordered_set<idx_t>({neighbor}));
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
	D_ASSERT(ret.size() == pow(2, neighbors.size()) - 1);
	for (auto &n : neighbors) {
		idx_t count = 0;
		for (auto &set : ret) {
			if (set.count(n) >= 1) {
				count += 1;
			}
		}
		D_ASSERT(count == pow(2, neighbors.size() - 1));
	}
#endif
	return ret;
}

void PlanEnumerator::GenerateCrossProducts() {
	// generate a set of cross products to combine the currently available plans into a full join plan
	// we create edges between every relation with a high cost
	for (idx_t i = 0; i < query_graph_manager.relation_manager.NumRelations(); i++) {
		auto left = query_graph_manager.set_manager.GetJoinRelation(i);
		for (idx_t j = 0; j < query_graph_manager.relation_manager.NumRelations(); j++) {
			if (i != j) {
				auto right = query_graph_manager.set_manager.GetJoinRelation(j);
				query_graph_manager.CreateQueryGraphCrossProduct(left, right);
			}
		}
	}
	// Now that the query graph has new edges, we need to re-initialize our query graph.
	// TODO: do we need to initialize our qyery graph again?
	// query_graph = query_graph_manager.GetQueryGraph();
}

//! Create a new JoinTree node by joining together two previous JoinTree nodes
unique_ptr<JoinNode> PlanEnumerator::CreateJoinTree(optional_ptr<JoinRelationSet> set,
                                                    const vector<reference<NeighborInfo>> &possible_connections,
                                                    optional_ptr<JoinNode> left, optional_ptr<JoinNode> right) {
	// for the hash join we want the right side (build side) to have the smallest cardinality
	// also just a heuristic but for now...
	// FIXME: we should probably actually benchmark that as well
	// FIXME: should consider different join algorithms, should we pick a join algorithm here as well? (probably)
	optional_ptr<NeighborInfo> best_connection = nullptr;
	if (possible_connections.size() > 0) {
		best_connection = &possible_connections.back().get();
	}
	idx_t cost = cost_model.ComputeCost(left, right);
	auto result = make_uniq<JoinNode>(set, best_connection, left, right, cost);
	result->cardinality = (idx_t)cost_model.cardinality_estimator.EstimateCardinalityWithSet(*set.get());
	return result;
}

JoinNode &PlanEnumerator::EmitPair(optional_ptr<JoinRelationSet> left, optional_ptr<JoinRelationSet> right,
                                   const vector<reference<NeighborInfo>> &info) {
	// get the left and right join plans
	auto left_plan = plans.find(left.get());
	auto right_plan = plans.find(right.get());
	if (left_plan == plans.end() || right_plan == plans.end()) {
		throw InternalException("No left or right plan: internal error in join order optimizer");
	}
	auto new_set = query_graph_manager.set_manager.Union(left, right);
	// create the join tree based on combining the two plans
	auto new_plan = CreateJoinTree(new_set, info, left_plan->second, right_plan->second);
	// check if this plan is the optimal plan we found for this set of relations
	auto entry = plans.find(new_set.get());
	auto new_cost = new_plan->cost;
	idx_t old_cost = NumericLimits<idx_t>::Maximum();
	if (entry != plans.end()) {
		old_cost = entry->second->cost;
	}
	if (entry == plans.end() || new_cost < old_cost) {
		// the new plan costs less than the old plan. Update our DP tree and cost tree
		auto &result = *new_plan;

		if (full_plan_found &&
		    join_nodes_in_full_plan.find(new_plan->set->ToString()) != join_nodes_in_full_plan.end()) {
			must_update_full_plan = true;
		}
		if (new_set->count == query_graph_manager.relation_manager.NumRelations()) {
			full_plan_found = true;
			// If we find a full plan, we need to keep track of which nodes are in the full plan.
			// It's possible the DP algorithm updates a node in the current full plan, then moves on
			// to the SolveApproximately. SolveApproximately may find a full plan with a higher cost than
			// what SolveExactly found. In this case, we revert to the SolveExactly plan, but it is
			// possible to get use-after-free errors if the SolveApproximately algorithm updated some (but not all)
			// nodes in the SolveExactly plan
			// If we know a node in the full plan is updated, we can prevent ourselves from exiting the
			// DP algorithm until the last plan updated is a full plan
			UpdateJoinNodesInFullPlan(result);
			if (must_update_full_plan) {
				must_update_full_plan = false;
			}
		}

		D_ASSERT(new_plan);
		plans[new_set.get()] = std::move(new_plan);
		return result;
	}
	return *entry->second;
}

bool PlanEnumerator::TryEmitPair(optional_ptr<JoinRelationSet> left, optional_ptr<JoinRelationSet> right,
                                 const vector<reference<NeighborInfo>> &info) {
	pairs++;
	// If a full plan is created, it's possible a node in the plan gets updated. When this happens, make sure you keep
	// emitting pairs until you emit another final plan. Another final plan is guaranteed to be produced because of
	// our symmetry guarantees.
	if (pairs >= 10000 && !must_update_full_plan) {
		// when the amount of pairs gets too large we exit the dynamic programming and resort to a greedy algorithm
		// FIXME: simple heuristic currently
		// at 10K pairs stop searching exactly and switch to heuristic
		return false;
	}
	EmitPair(left, right, info);
	return true;
}

bool PlanEnumerator::EmitCSG(optional_ptr<JoinRelationSet> node) {
	if (node->count == query_graph_manager.relation_manager.NumRelations()) {
		return true;
	}
	// create the exclusion set as everything inside the subgraph AND anything with members BELOW it
	unordered_set<idx_t> exclusion_set;
	for (idx_t i = 0; i < node->relations[0]; i++) {
		exclusion_set.insert(i);
	}
	UpdateExclusionSet(node.get(), exclusion_set);
	// find the neighbors given this exclusion set
	auto neighbors = query_graph.GetNeighbors(*node, exclusion_set);
	if (neighbors.empty()) {
		return true;
	}

	//! Neighbors should be reversed when iterating over them.
	std::sort(neighbors.begin(), neighbors.end(), std::greater_equal<idx_t>());
	for (idx_t i = 0; i < neighbors.size() - 1; i++) {
		D_ASSERT(neighbors[i] > neighbors[i + 1]);
	}

	// Dphyp paper missiing this.
	// Because we are traversing in reverse order, we need to add neighbors whose number is smaller than the current
	// node to exclusion_set
	// This avoids duplicated enumeration
	unordered_set<idx_t> new_exclusion_set = exclusion_set;
	for (idx_t i = 0; i < neighbors.size(); ++i) {
		D_ASSERT(new_exclusion_set.find(neighbors[i]) == new_exclusion_set.end());
		new_exclusion_set.insert(neighbors[i]);
	}

	for (auto neighbor : neighbors) {
		// since the GetNeighbors only returns the smallest element in a list, the entry might not be connected to
		// (only!) this neighbor,  hence we have to do a connectedness check before we can emit it
		auto neighbor_relation = query_graph_manager.set_manager.GetJoinRelation(neighbor);
		auto connections = query_graph.GetConnections(node, neighbor_relation);
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

bool PlanEnumerator::EnumerateCmpRecursive(optional_ptr<JoinRelationSet> left, optional_ptr<JoinRelationSet> right,
                                           unordered_set<idx_t> &exclusion_set) {
	// get the neighbors of the second relation under the exclusion set
	auto neighbors = query_graph.GetNeighbors(*right, exclusion_set);
	if (neighbors.empty()) {
		return true;
	}

	auto all_subset = GetAllNeighborSets(neighbors);
	vector<optional_ptr<JoinRelationSet>> union_sets;
	union_sets.reserve(all_subset.size());
	for (const auto &rel_set : all_subset) {
		auto neighbor = query_graph_manager.set_manager.GetJoinRelation(rel_set);
		// emit the combinations of this node and its neighbors
		auto combined_set = query_graph_manager.set_manager.Union(right, neighbor);
		// If combined_set.count == right.count, This means we found a neighbor that has been present before
		// This means we didn't set exclusion_set correctly.
		D_ASSERT(combined_set->count > right->count);
		if (plans.find(combined_set.get()) != plans.end()) {
			auto connections = query_graph.GetConnections(left, combined_set);
			if (!connections.empty()) {
				if (!TryEmitPair(left, combined_set, connections)) {
					return false;
				}
			}
		}
		union_sets.push_back(combined_set);
	}

	unordered_set<idx_t> new_exclusion_set = exclusion_set;
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

bool PlanEnumerator::EnumerateCSGRecursive(optional_ptr<JoinRelationSet> node, unordered_set<idx_t> &exclusion_set) {
	// find neighbors of S under the exclusion set
	auto neighbors = query_graph.GetNeighbors(*node, exclusion_set);
	if (neighbors.empty()) {
		return true;
	}

	auto all_subset = GetAllNeighborSets(neighbors);
	vector<optional_ptr<JoinRelationSet>> union_sets;
	union_sets.reserve(all_subset.size());
	for (const auto &rel_set : all_subset) {
		auto neighbor = query_graph_manager.set_manager.GetJoinRelation(rel_set);
		// emit the combinations of this node and its neighbors
		auto new_set = query_graph_manager.set_manager.Union(node, neighbor);
		D_ASSERT(new_set->count > node->count);
		if (plans.find(new_set.get()) != plans.end()) {
			if (!EmitCSG(new_set)) {
				return false;
			}
		}
		union_sets.push_back(new_set);
	}

	unordered_set<idx_t> new_exclusion_set = exclusion_set;
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

bool PlanEnumerator::SolveJoinOrderExactly() {
	// now we perform the actual dynamic programming to compute the final result
	// we enumerate over all the possible pairs in the neighborhood
	for (idx_t i = query_graph_manager.relation_manager.NumRelations(); i > 0; i--) {
		// for every node in the set, we consider it as the start node once
		auto start_node = query_graph_manager.set_manager.GetJoinRelation(i - 1);
		// emit the start node
		if (!EmitCSG(start_node)) {
			return false;
		}
		// initialize the set of exclusion_set as all the nodes with a number below this
		unordered_set<idx_t> exclusion_set;
		for (idx_t j = 0; j < i; j++) {
			exclusion_set.insert(j);
		}
		// then we recursively search for neighbors that do not belong to the banned entries
		if (!EnumerateCSGRecursive(start_node, exclusion_set)) {
			return false;
		}
	}
	return true;
}

void PlanEnumerator::UpdateDPTree(JoinNode &new_plan) {
	if (!NodeInFullPlan(new_plan)) {
		// if the new node is not in the full plan, feel free to return
		// because you won't be updating the full plan.
		return;
	}
	auto &new_set = new_plan.set;
	// now update every plan that uses this plan
	unordered_set<idx_t> exclusion_set;
	for (idx_t i = 0; i < new_set->count; i++) {
		exclusion_set.insert(new_set->relations[i]);
	}
	auto neighbors = query_graph.GetNeighbors(*new_set, exclusion_set);
	auto all_neighbors = GetAllNeighborSets(neighbors);
	for (const auto &neighbor : all_neighbors) {
		auto neighbor_relation = query_graph_manager.set_manager.GetJoinRelation(neighbor);
		auto combined_set = query_graph_manager.set_manager.Union(new_set, neighbor_relation);

		auto combined_set_plan = plans.find(combined_set.get());
		if (combined_set_plan == plans.end()) {
			continue;
		}

		double combined_set_plan_cost = combined_set_plan->second->cost; // combined_set_plan->second->GetCost();
		auto connections = query_graph.GetConnections(new_set, neighbor_relation);
		// recurse and update up the tree if the combined set produces a plan with a lower cost
		// only recurse on neighbor relations that have plans.
		auto right_plan = plans.find(neighbor_relation.get());
		if (right_plan == plans.end()) {
			continue;
		}
		auto &updated_plan = EmitPair(new_set, neighbor_relation, connections);
		// <= because the child node has already been replaced. You need to
		// replace the parent node as well in this case
		if (updated_plan.cost < combined_set_plan_cost) {
			UpdateDPTree(updated_plan);
		}
	}
}

void PlanEnumerator::SolveJoinOrderApproximately() {
	// at this point, we exited the dynamic programming but did not compute the final join order because it took too
	// long instead, we use a greedy heuristic to obtain a join ordering now we use Greedy Operator Ordering to
	// construct the result tree first we start out with all the base relations (the to-be-joined relations)
	vector<optional_ptr<JoinRelationSet>> join_relations; // T in the paper
	for (idx_t i = 0; i < query_graph_manager.relation_manager.NumRelations(); i++) {
		join_relations.push_back(query_graph_manager.set_manager.GetJoinRelation(i));
	}
	while (join_relations.size() > 1) {
		// now in every step of the algorithm, we greedily pick the join between the to-be-joined relations that has the
		// smallest cost. This is O(r^2) per step, and every step will reduce the total amount of relations to-be-joined
		// by 1, so the total cost is O(r^3) in the amount of relations
		idx_t best_left = 0, best_right = 0;
		optional_ptr<JoinNode> best_connection;
		for (idx_t i = 0; i < join_relations.size(); i++) {
			auto left = join_relations[i];
			for (idx_t j = i + 1; j < join_relations.size(); j++) {
				auto right = join_relations[j];
				// check if we can connect these two relations
				auto connection = query_graph.GetConnections(left, right);
				if (!connection.empty()) {
					// we can check the cost of this connection
					auto &node = EmitPair(left, right, connection);

					// update the DP tree in case a plan created by the DP algorithm uses the node
					// that was potentially just updated by EmitPair. You will get a use-after-free
					// error if future plans rely on the old node that was just replaced.
					// if node in FullPath, then updateDP tree.
					UpdateDPTree(node);

					if (!best_connection || node.cost < best_connection->cost) {
						// best pair found so far
						best_connection = &node;
						best_left = i;
						best_right = j;
					}
				}
			}
		}
		if (!best_connection) {
			// could not find a connection, but we were not done with finding a completed plan
			// we have to add a cross product; we add it between the two smallest relations
			optional_ptr<JoinNode> smallest_plans[2];
			idx_t smallest_index[2];
			D_ASSERT(join_relations.size() >= 2);

			// first just add the first two join relations. It doesn't matter the cost as the JOO
			// will swap them on estimated cardinality anyway.
			for (idx_t i = 0; i < 2; i++) {
				auto current_plan = plans[join_relations[i].get()].get();
				smallest_plans[i] = current_plan;
				smallest_index[i] = i;
			}

			// if there are any other join relations that don't have connections
			// add them if they have lower estimated cardinality.
			for (idx_t i = 2; i < join_relations.size(); i++) {
				// get the plan for this relation
				auto current_plan = plans[join_relations[i].get()].get();
				// check if the cardinality is smaller than the smallest two found so far
				for (idx_t j = 0; j < 2; j++) {
					if (!smallest_plans[j] || smallest_plans[j]->cost > current_plan->cost) {
						smallest_plans[j] = current_plan;
						smallest_index[j] = i;
						break;
					}
				}
			}
			if (!smallest_plans[0] || !smallest_plans[1]) {
				throw InternalException("Internal error in join order optimizer");
			}
			D_ASSERT(smallest_plans[0] && smallest_plans[1]);
			D_ASSERT(smallest_index[0] != smallest_index[1]);
			auto &left = smallest_plans[0]->set;
			auto &right = smallest_plans[1]->set;
			// create a cross product edge (i.e. edge with empty filter) between these two sets in the query graph
			query_graph_manager.CreateQueryGraphCrossProduct(left, right);
			//			query_graph = query_graph_manager.GetQueryGraph();
			// now emit the pair and continue with the algorithm
			auto connections = query_graph.GetConnections(left, right);
			D_ASSERT(!connections.empty());

			best_connection = &EmitPair(left, right, connections);
			best_left = smallest_index[0];
			best_right = smallest_index[1];

			UpdateDPTree(*best_connection);
			// the code below assumes best_right > best_left
			if (best_left > best_right) {
				std::swap(best_left, best_right);
			}
		}
		// now update the to-be-checked pairs
		// remove left and right, and add the combination

		// important to erase the biggest element first
		// if we erase the smallest element first the index of the biggest element changes
		D_ASSERT(best_right > best_left);
		join_relations.erase(join_relations.begin() + best_right);
		join_relations.erase(join_relations.begin() + best_left);
		join_relations.push_back(best_connection->set);
	}
}

void PlanEnumerator::InitLeafPlans() {
	// First we initialize each of the single-node plans with themselves and with their cardinalities these are the leaf
	// nodes of the join tree NOTE: we can just use pointers to JoinRelationSet* here because the GetJoinRelation
	// function ensures that a unique combination of relations will have a unique JoinRelationSet object.
	vector<NodeOp> nodes_ops;
	// first initialize equivalent relations based on the filters
	auto relation_stats = query_graph_manager.relation_manager.GetRelationStats();

	cost_model.cardinality_estimator.InitEquivalentRelations(query_graph_manager.GetFilterBindings());
	cost_model.cardinality_estimator.AddRelationNamesToTdoms(relation_stats);

	// then update the total domains based on the cardinalities of each relation.
	for (idx_t i = 0; i < relation_stats.size(); i++) {
		auto stats = relation_stats.at(i);
		auto relation_set = query_graph_manager.set_manager.GetJoinRelation(i);
		auto join_node = make_uniq<JoinNode>(relation_set);
		join_node->cost = 0;
		join_node->cardinality = stats.cardinality;
		plans[relation_set.get()] = std::move(join_node);
		stats.filter_strength = 1;
		cost_model.cardinality_estimator.InitCardinalityEstimatorProps(relation_set, stats);
	}

}

unique_ptr<JoinNode> PlanEnumerator::SolveJoinOrder(bool force_no_cross_product) {
	// first try to solve the join order exactly
	if (!SolveJoinOrderExactly()) {
		// otherwise, if that times out we resort to a greedy algorithm
		SolveJoinOrderApproximately();
	}

	// now the optimal join path should have been found
	// get it from the node
	unordered_set<idx_t> bindings;
	for (idx_t i = 0; i < query_graph_manager.relation_manager.NumRelations(); i++) {
		bindings.insert(i);
	}
	auto total_relation = query_graph_manager.set_manager.GetJoinRelation(bindings);
	auto final_plan = plans.find(total_relation.get());
	if (final_plan == plans.end()) {
		// could not find the final plan
		// this should only happen in case the sets are actually disjunct
		// in this case we need to generate cross product to connect the disjoint sets
		if (force_no_cross_product) {
			throw InvalidInputException(
			    "Query requires a cross-product, but 'force_no_cross_product' PRAGMA is enabled");
		}
		GenerateCrossProducts();
		//! solve the join order again, returning the final plan
		return SolveJoinOrder(force_no_cross_product);
	}
	return std::move(final_plan->second);
}

//unique_ptr<LogicalOperator> PlanEnumerator::BuildSideProbeSideSwaps(unique_ptr<LogicalOperator> plan) {
//	if (plan->children.size() == 1) {
//		return BuildSideProbeSideSwaps(std::move(plan->children.at(0)));
//	}
//	else if (plan->children.size() == 2) {
//
//	}
//	if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
//			auto &join = op->Cast<LogicalComparisonJoin>();
//			if (join.join_type == JoinType::LEFT && join.right_projection_map.empty()) {
//				// for left joins; if the RHS cardinality is significantly larger than the LHS (2x)
//				// we convert to doing a RIGHT OUTER JOIN
//				// FIXME: for now we don't swap if the right_projection_map is not empty
//				// this can be fixed once we implement the left_projection_map properly...
//				auto lhs_cardinality = join.children[0]->EstimateCardinality(context);
//				auto rhs_cardinality = join.children[1]->EstimateCardinality(context);
//				if (rhs_cardinality > lhs_cardinality * 2) {
//					join.join_type = JoinType::RIGHT;
//					std::swap(join.children[0], join.children[1]);
//					for (auto &cond : join.conditions) {
//						std::swap(cond.left, cond.right);
//						cond.comparison = FlipComparisonExpression(cond.comparison);
//					}
//				}
//			}
//		}
//	if (op->type == LogicalOperatorType::LOGICAL_ANY_JOIN && non_reorderable_operation) {
//		auto &join = op->Cast<LogicalAnyJoin>();
//		if (join.join_type == JoinType::LEFT && join.right_projection_map.empty()) {
//			auto lhs_cardinality = join.children[0]->EstimateCardinality(context);
//			auto rhs_cardinality = join.children[1]->EstimateCardinality(context);
//			if (rhs_cardinality > lhs_cardinality * 2) {
//				join.join_type = JoinType::RIGHT;
//				std::swap(join.children[0], join.children[1]);
//			}
//		}
//	}

//}


} // namespace duckdb

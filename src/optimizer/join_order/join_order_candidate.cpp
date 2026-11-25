#include "duckdb/optimizer/join_order/join_order_candidate.hpp"

#include "duckdb/optimizer/join_order/query_graph_manager.hpp"

namespace duckdb {

JoinOrderCandidateEvaluator::JoinOrderCandidateEvaluator(QueryGraphManager &graph, CostModel &cost_model,
                                                         const QueryGraphEdges &edges)
    : query_graph_manager(graph), cost_model(cost_model), query_graph(edges) {
}

JoinPlanCost JoinOrderCandidateEvaluator::GetPlanCost(const DPJoinNode &node) const {
	return cost_model.GetCost(node);
}

const vector<reference<NeighborInfo>> &JoinOrderCandidateEvaluator::GetConnections(JoinRelationSet &left,
                                                                                   JoinRelationSet &right) {
	auto &left_cache = connection_cache[left];
	auto entry = left_cache.find(right);
	if (entry != left_cache.end()) {
		return entry->second;
	}
	auto connections = query_graph.GetConnections(left, right);
	auto inserted = left_cache.insert(make_pair(reference<JoinRelationSet>(right), std::move(connections)));
	return inserted.first->second;
}

static bool EnablesJoinKey(const JoinPredicateModel &predicates, JoinRelationSet &set, JoinRelationSet &left,
                           JoinRelationSet &right) {
	auto needs_both_inputs = [&](optional_ptr<JoinRelationSet> key) {
		return key && JoinRelationSet::Intersects(left, *key) && JoinRelationSet::Intersects(right, *key);
	};
	for (auto &predicate_ref : predicates.GetGraphPredicates()) {
		auto &predicate = predicate_ref.get();
		if (!JoinRelationSet::IsSubset(set, predicate.GetSet()) &&
		    (needs_both_inputs(predicate.GetLeftSetOptional()) || needs_both_inputs(predicate.GetRightSetOptional()))) {
			return true;
		}
	}
	return false;
}

unique_ptr<DPJoinNode>
JoinOrderCandidateEvaluator::CreateJoinTree(JoinRelationSet &set,
                                            const vector<reference<NeighborInfo>> &possible_connections,
                                            DPJoinNode &left, DPJoinNode &right) {
	// FIXME: should consider different join algorithms, should we pick a join algorithm here as well? (probably)
	vector<reference<NeighborInfo>> applicable_connections;
	struct JoinCandidate {
		optional_ptr<JoinOrderOperator> join_operator;
		vector<reference<JoinPredicate>> predicates;
		bool generated_cross_product = false;
		idx_t priority = 0;
	};
	vector<JoinCandidate> candidates;
	for (auto &connection_ref : possible_connections) {
		auto &connection = connection_ref.get();
		if (!query_graph_manager.IsConnectionApplicable(connection, left.set, right.set)) {
			continue;
		}
		applicable_connections.push_back(connection);
		optional_ptr<JoinCandidate> candidate;
		for (auto &entry : candidates) {
			if (entry.join_operator == connection.join_operator &&
			    entry.generated_cross_product == connection.generated_cross_product) {
				candidate = entry;
				break;
			}
		}
		if (!candidate) {
			candidates.emplace_back();
			candidate = candidates.back();
			candidate->join_operator = connection.join_operator;
			candidate->generated_cross_product = connection.generated_cross_product;
		}
		candidate->predicates.insert(candidate->predicates.end(), connection.predicates.begin(),
		                             connection.predicates.end());
		idx_t priority = 0;
		if (!connection.predicates.empty()) {
			priority = 3;
		} else if (connection.join_operator) {
			priority = 2;
		} else if (connection.generated_cross_product) {
			priority = 1;
		}
		candidate->priority = MaxValue(candidate->priority, priority);
	}
	if (applicable_connections.empty()) {
		return nullptr;
	}
	optional_ptr<JoinCandidate> best_candidate;
	for (auto &candidate : candidates) {
		if (!query_graph_manager.IsJoinOrderCandidate(candidate.join_operator, candidate.generated_cross_product,
		                                              left.set, right.set)) {
			continue;
		}
		if (!best_candidate || candidate.priority > best_candidate->priority) {
			best_candidate = candidate;
		}
	}
	if (!best_candidate) {
		return nullptr;
	}
	auto cardinality = cost_model.GetCardinalityEstimator().EstimateCardinality(set);
	// Avoid large expanding independent builds unless the inputs contribute to the same join key.
	if (recursive_frontier_estimate > 0 && !left.recursive_properties.depends_on_recursive_input &&
	    !right.recursive_properties.depends_on_recursive_input) {
		auto output = cardinality.GetDouble();
		if (output > left.cardinality.GetDouble() && output > right.cardinality.GetDouble() &&
		    output > recursive_frontier_estimate &&
		    !EnablesJoinKey(query_graph_manager.GetPredicateModel(), set, left.set, right.set)) {
			return nullptr;
		}
	}
	auto cost = cost_model.ComputeCost(left, right, best_candidate->join_operator, cardinality.GetDouble());
	auto result = make_uniq<DPJoinNode>(set, best_candidate->join_operator, std::move(best_candidate->predicates),
	                                    best_candidate->generated_cross_product, left.set, right.set, cost);
	result->cardinality = cardinality;
	cost_model.UpdateBuildMemory(*result, left, right);
	return result;
}

optional_ptr<DPJoinNode> JoinOrderCandidateEvaluator::EmitPair(JoinOrderPlanMap &plans, JoinRelationSet &left,
                                                               JoinRelationSet &right,
                                                               const vector<reference<NeighborInfo>> &info) {
	auto &set = query_graph_manager.set_manager.Union(left, right);
	// get the left and right join plans
	auto left_plan = plans.find(left);
	auto right_plan = plans.find(right);
	if (left_plan == plans.end() || right_plan == plans.end()) {
		throw InternalException("No left or right plan: internal error in join order optimizer");
	}
	// create the join tree based on combining the two plans
	return ConsiderJoinTree(plans, CreateJoinTree(set, info, *left_plan->second, *right_plan->second));
}

optional_ptr<DPJoinNode> JoinOrderCandidateEvaluator::ConsiderJoinTree(JoinOrderPlanMap &plans,
                                                                       unique_ptr<DPJoinNode> new_plan) {
	if (!new_plan) {
		return nullptr;
	}
	auto &new_set = new_plan->set;
	// check if this plan is the optimal plan we found for this set of relations
	auto entry = plans.find(new_set);
	auto new_cost = GetPlanCost(*new_plan);
	JoinPlanCost old_cost {NumericLimits<double>::Maximum(), NumericLimits<double>::Maximum()};
	if (entry != plans.end()) {
		old_cost = GetPlanCost(*entry->second);
	}
	if (entry == plans.end() || new_cost < old_cost) {
		// the new plan costs less than the old plan. Update our DP table.
		plans[new_set] = std::move(new_plan);
		return plans[new_set].get();
	}
	// Tiebreaker for equal-cost plans: needed for LEFT JOINs,
	// because all orderings preserve the LHS cardinality and always tie.
	// This tiebreaker causes less joins to be flipped from LEFT to RIGHT
	// later by the BuildProbeSideOptimizer, and we strongly prefer LEFT
	if (new_cost == old_cost) {
		auto new_right_cardinality = plans.at(new_plan->right_set)->cardinality;
		auto existing_right = plans.find(entry->second->right_set);
		if (existing_right != plans.end()) {
			auto old_right_cardinality = existing_right->second->cardinality;
			if (new_right_cardinality > old_right_cardinality) {
				plans[new_set] = std::move(new_plan);
				return plans[new_set].get();
			}
		}
	}
	// Create join node from the plan currently in the DP table.
	return entry->second.get();
}

void JoinOrderCandidateEvaluator::InitLeafPlans(JoinOrderPlanMap &plans) {
	auto relation_stats = query_graph_manager.relation_manager.GetRelationStats();
	auto &estimator = cost_model.GetCardinalityEstimator();
	estimator.Initialize(relation_stats, query_graph_manager.GetJoinOperators(),
	                     query_graph_manager.relation_manager.GetColumnSources());
	for (idx_t i = 0; i < relation_stats.size(); i++) {
		auto &relation_set = query_graph_manager.set_manager.GetJoinRelation(RelationIndex(i));
		auto join_node = make_uniq<DPJoinNode>(relation_set);
		join_node->cost = 0;
		join_node->cardinality = estimator.EstimateCardinality(relation_set);
		if (query_graph_manager.relation_manager.HasRecursiveRelations()) {
			auto &relation = query_graph_manager.relation_manager.GetRelation(RelationIndex(i));
			relation.op.ResolveOperatorTypes();
			join_node->recursive_properties = relation.recursive_properties;
			if (join_node->recursive_properties.depends_on_recursive_input) {
				recursive_frontier_estimate = MaxValue(recursive_frontier_estimate, join_node->cardinality.GetDouble());
			}
		}
		plans[relation_set] = std::move(join_node);
	}
}

void JoinOrderCandidateEvaluator::InitInputPlan(JoinOrderPlanMap &plans) {
	for (auto &op : query_graph_manager.GetJoinOperators()) {
		auto &left = op->left_relations.get();
		auto &right = op->right_relations.get();
		if (plans.find(left) == plans.end() || plans.find(right) == plans.end()) {
			continue;
		}
		auto &connections = GetConnections(left, right);
		if (!connections.empty()) {
			EmitPair(plans, left, right, connections);
		}
	}
}

bool JoinOrderCandidateEvaluator::ActivateRequiredCrossProducts() {
	if (!query_graph_manager.ActivateRequiredCrossProducts()) {
		return false;
	}
	connection_cache.clear();
	return true;
}

const vector<reference<NeighborInfo>> &JoinOrderCandidateEvaluator::CreateCrossProduct(JoinRelationSet &left,
                                                                                       JoinRelationSet &right) {
	query_graph_manager.CreateQueryGraphCrossProduct(left, right);
	connection_cache.clear();
	return GetConnections(left, right);
}

} // namespace duckdb

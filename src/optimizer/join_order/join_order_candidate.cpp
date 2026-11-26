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
	auto cost = cost_model.ComputeCost(left, right, best_candidate->join_operator, set);
	auto result = make_uniq<DPJoinNode>(set, best_candidate->join_operator, std::move(best_candidate->predicates),
	                                    best_candidate->generated_cross_product, left.set, right.set, cost);
	result->cardinality = cardinality;
	cost_model.UpdateBuildMemory(*result, left, right);
	return result;
}

optional_ptr<DPJoinNode> JoinOrderCandidateEvaluator::EmitPair(JoinOrderPlanMap &plans, JoinRelationSet &left,
                                                               JoinRelationSet &right,
                                                               const vector<reference<NeighborInfo>> &connections) {
	auto &set = query_graph_manager.set_manager.Union(left, right);
	return EmitPair(plans, set, left, right, connections);
}

optional_ptr<DPJoinNode> JoinOrderCandidateEvaluator::EmitPair(JoinOrderPlanMap &plans, JoinRelationSet &set,
                                                               JoinRelationSet &left, JoinRelationSet &right,
                                                               const vector<reference<NeighborInfo>> &connections) {
	// get the left and right join plans
	auto left_plan = plans.find(left);
	auto right_plan = plans.find(right);
	if (left_plan == plans.end() || right_plan == plans.end()) {
		throw InternalException("No left or right plan: internal error in join order optimizer");
	}
	// create the join tree based on combining the two plans
	return ConsiderJoinTree(plans, CreateJoinTree(set, connections, *left_plan->second, *right_plan->second));
}

optional_ptr<DPJoinNode> JoinOrderCandidateEvaluator::ConsiderJoinTree(JoinOrderPlanMap &plans,
                                                                       unique_ptr<DPJoinNode> new_plan) {
	if (!new_plan) {
		return nullptr;
	}
	auto &new_set = new_plan->set;
	// check if this plan is the optimal plan we found for this set of relations
	auto entry = plans.find(new_set);
	if (entry == plans.end() || PreferPlan(*new_plan, plans, *entry->second, plans)) {
		plans[new_set] = std::move(new_plan);
		return plans[new_set].get();
	}
	return entry->second.get();
}

bool JoinOrderCandidateEvaluator::PreferPlan(const DPJoinNode &candidate, const JoinOrderPlanMap &candidate_plans,
                                             const DPJoinNode &incumbent,
                                             const JoinOrderPlanMap &incumbent_plans) const {
	auto new_cost = cost_model.GetCost(candidate);
	auto old_cost = cost_model.GetCost(incumbent);
	if (new_cost < old_cost) {
		return true;
	}
	// Tiebreaker for equal-cost plans: needed for LEFT JOINs,
	// because all orderings preserve the LHS cardinality and always tie.
	// This tiebreaker causes less joins to be flipped from LEFT to RIGHT
	// later by the BuildProbeSideOptimizer, and we strongly prefer LEFT
	if (new_cost == old_cost && !candidate.is_leaf) {
		auto new_right_cardinality = candidate_plans.at(candidate.right_set)->cardinality;
		auto existing_right = incumbent_plans.find(incumbent.right_set);
		if (existing_right != incumbent_plans.end()) {
			auto old_right_cardinality = existing_right->second->cardinality;
			if (new_right_cardinality > old_right_cardinality) {
				return true;
			}
		}
	}
	return false;
}

bool JoinOrderCandidateEvaluator::CanImprovePair(const JoinOrderPlanMap &plans, const DPJoinNode &left,
                                                 const DPJoinNode &right, const DPJoinNode &incumbent) const {
	auto incumbent_cost = cost_model.GetCost(incumbent);
	if (incumbent_cost.memory_excess > 0) {
		return true;
	}
	auto minimum_work = cost_model.ComputeCostLowerBound(left, right, incumbent.cardinality.GetDouble());
	if (minimum_work > incumbent_cost.work) {
		return false;
	}
	if (minimum_work != incumbent_cost.work) {
		return true;
	}
	auto incumbent_right = plans.find(incumbent.right_set);
	if (incumbent_right == plans.end()) {
		return true;
	}
	// Equal work can still win PreferPlan's right-cardinality tiebreaker.
	return left.cardinality > incumbent_right->second->cardinality ||
	       right.cardinality > incumbent_right->second->cardinality;
}

void JoinOrderCandidateEvaluator::InitLeafPlans(JoinOrderPlanMap &plans, vector<RelationStats> relation_stats) {
	auto &estimator = cost_model.GetCardinalityEstimator();
	estimator.Initialize(std::move(relation_stats), query_graph_manager.GetJoinOperators(),
	                     query_graph_manager.relation_manager.GetColumnSources());
	for (idx_t i = 0; i < query_graph_manager.relation_manager.NumRelations(); i++) {
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

void JoinOrderCandidateEvaluator::ResetConnectionCache() {
	connection_cache.clear();
}

} // namespace duckdb

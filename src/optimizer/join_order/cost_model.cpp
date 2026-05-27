#include "duckdb/optimizer/join_order/join_node.hpp"
#include "duckdb/optimizer/join_order/join_order_optimizer.hpp"
#include "duckdb/optimizer/join_order/cost_model.hpp"

#include "duckdb/optimizer/join_order/query_graph_manager.hpp"

namespace duckdb {

CostModel::CostModel(QueryGraphManager &query_graph_manager, CardinalityEstimator &cardinality_estimator)
    : query_graph_manager(query_graph_manager), cardinality_estimator(cardinality_estimator) {
}

CardinalityEstimator &CostModel::GetCardinalityEstimator() {
	return cardinality_estimator;
}

static double GetLeftJoinInputCost(CardinalityEstimator &cardinality_estimator,
                                   const vector<reference<NeighborInfo>> &possible_connections) {
	double cost = 0;
	reference_set_t<JoinRelationSet> seen_right_sides;
	for (auto &connection : possible_connections) {
		for (auto predicate_ref : connection.get().predicates) {
			auto &predicate = predicate_ref.get();
			if (predicate.GetJoinType() != JoinType::LEFT) {
				continue;
			}
			D_ASSERT(predicate.GetRightSetOptional());
			if (!seen_right_sides.insert(predicate.GetRightSet()).second) {
				continue;
			}
			cost += cardinality_estimator.EstimateCardinalityWithSet<double>(predicate.GetRightSet());
		}
	}
	return cost;
}

static optional_ptr<JoinRelationSet> GetContainingChild(DPJoinNode &left, DPJoinNode &right, JoinRelationSet &set) {
	if (JoinRelationSet::IsSubset(left.set, set)) {
		return &left.set;
	}
	if (JoinRelationSet::IsSubset(right.set, set)) {
		return &right.set;
	}
	return nullptr;
}

static double GetLeftJoinDeferredInnerCost(QueryGraphManager &query_graph_manager,
                                           CardinalityEstimator &cardinality_estimator, DPJoinNode &left,
                                           DPJoinNode &right, JoinRelationSet &combination,
                                           const vector<reference<NeighborInfo>> &possible_connections) {
	static constexpr double DEFERRED_INNER_WORK_WEIGHT = 4;
	double cost = 0;
	reference_set_t<JoinRelationSet> seen_pending_sides;
	for (auto &connection : possible_connections) {
		for (auto left_predicate_ref : connection.get().predicates) {
			auto &left_predicate = left_predicate_ref.get();
			if (left_predicate.GetJoinType() != JoinType::LEFT) {
				continue;
			}
			D_ASSERT(left_predicate.GetLeftSetOptional() && left_predicate.GetRightSetOptional());
			auto lhs_child = GetContainingChild(left, right, left_predicate.GetLeftSet());
			auto rhs_child = GetContainingChild(left, right, left_predicate.GetRightSet());
			if (!lhs_child || !rhs_child || lhs_child == rhs_child) {
				continue;
			}

			for (auto pending_predicate_ref : query_graph_manager.GetPredicateModel().GetEqualityJoinPredicates()) {
				auto &pending_predicate = pending_predicate_ref.get();
				D_ASSERT(pending_predicate.GetLeftSetOptional() && pending_predicate.GetRightSetOptional());
				if (JoinRelationSet::IsSubset(*lhs_child, pending_predicate.GetSet())) {
					continue;
				}

				auto left_inside = JoinRelationSet::IsSubset(*lhs_child, pending_predicate.GetLeftSet());
				auto right_inside = JoinRelationSet::IsSubset(*lhs_child, pending_predicate.GetRightSet());
				if (left_inside == right_inside) {
					continue;
				}

				auto pending_side =
				    left_inside ? pending_predicate.GetRightSetOptional() : pending_predicate.GetLeftSetOptional();
				if (JoinRelationSet::IsSubset(combination, *pending_side)) {
					continue;
				}
				if (!seen_pending_sides.insert(*pending_side).second) {
					continue;
				}
				auto pending_side_card = cardinality_estimator.EstimateCardinalityWithSet<double>(*pending_side);
				auto &future_with_left = query_graph_manager.set_manager.Union(combination, *pending_side);
				auto future_with_left_card = cardinality_estimator.EstimateCardinalityWithSet<double>(future_with_left);
				cost += pending_side_card + (future_with_left_card * DEFERRED_INNER_WORK_WEIGHT);
			}
		}
	}
	return cost;
}

// Currently cost of a join mostly factors in the cardinalities.
// LEFT joins need an explicit RHS input component because their output cardinality preserves the LHS,
// which otherwise makes early LEFT joins over large RHS inputs look almost free.
double CostModel::ComputeCost(DPJoinNode &left, DPJoinNode &right, JoinRelationSet &combination,
                              const vector<reference<NeighborInfo>> &possible_connections) {
	auto join_card = cardinality_estimator.EstimateCardinalityWithSet<double>(combination);
	auto join_cost = join_card;
	if (query_graph_manager.GetPredicateModel().HasLeftJoinPredicates()) {
		join_cost += GetLeftJoinInputCost(cardinality_estimator, possible_connections) +
		             GetLeftJoinDeferredInnerCost(query_graph_manager, cardinality_estimator, left, right, combination,
		                                          possible_connections);
	}
	return join_cost + left.cost + right.cost;
}

} // namespace duckdb

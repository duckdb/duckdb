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

// Currently cost of a join mostly factors in the cardinalities.
// LEFT joins need an explicit RHS input component because their output cardinality preserves the LHS,
// which otherwise makes early LEFT joins over large RHS inputs look almost free.
double CostModel::ComputeCost(DPJoinNode &left, DPJoinNode &right, JoinRelationSet &combination,
                              const vector<reference<NeighborInfo>> &possible_connections) {
	auto join_card = cardinality_estimator.EstimateCardinalityWithSet<double>(combination);
	auto join_cost = join_card;
	if (query_graph_manager.GetPredicateModel().HasLeftJoinPredicates()) {
		join_cost += GetLeftJoinInputCost(cardinality_estimator, possible_connections);
	}
	return join_cost + left.cost + right.cost;
}

} // namespace duckdb

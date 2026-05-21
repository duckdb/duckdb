#include "duckdb/optimizer/join_order/join_node.hpp"
#include "duckdb/optimizer/join_order/join_order_optimizer.hpp"
#include "duckdb/optimizer/join_order/cost_model.hpp"
#include "duckdb/optimizer/join_order/query_graph_manager.hpp"

namespace duckdb {

static bool HasCardinalityPreservingJoin(QueryGraphManager &query_graph_manager);

CostModel::CostModel(QueryGraphManager &query_graph_manager, CardinalityEstimator &cardinality_estimator)
    : query_graph_manager(query_graph_manager),
      has_cardinality_preserving_join(HasCardinalityPreservingJoin(query_graph_manager)),
      cardinality_estimator(cardinality_estimator) {
}

CardinalityEstimator &CostModel::GetCardinalityEstimator() {
	return cardinality_estimator;
}

static double GetLeftJoinInputCost(CardinalityEstimator &cardinality_estimator,
                                   const vector<reference<NeighborInfo>> &possible_connections) {
	double cost = 0;
	unordered_set<string> seen_right_sides;
	for (auto &connection : possible_connections) {
		for (auto &filter : connection.get().filters) {
			if (filter->join_type != JoinType::LEFT || !filter->right_set) {
				continue;
			}
			auto right_side = filter->right_set->ToString();
			if (!seen_right_sides.insert(right_side).second) {
				continue;
			}
			cost += cardinality_estimator.EstimateCardinalityWithSet<double>(*filter->right_set);
		}
	}
	return cost;
}

static bool Contains(JoinRelationSet &super, optional_ptr<JoinRelationSet> sub) {
	return sub && !sub->Empty() && JoinRelationSet::IsSubset(super, *sub);
}

static bool HasCardinalityPreservingJoin(QueryGraphManager &query_graph_manager) {
	for (auto &filter : query_graph_manager.GetFilterBindings()) {
		if (JoinOrderUtil::IsCardinalityPreservingJoinPredicate(*filter)) {
			return true;
		}
	}
	return false;
}

static double GetPendingSmallSidePenalty(QueryGraphManager &query_graph_manager,
                                         CardinalityEstimator &cardinality_estimator, JoinRelationSet &combination,
                                         bool has_cardinality_preserving_join) {
	// Cardinality-preserving joins can make it look cheap to build a huge intermediate before applying a selective
	// equality edge to a tiny dimension. This is a narrow input-work proxy: penalize that shape only when a large
	// pending side is several orders of magnitude larger than the relation it can still join to.
	static constexpr double LARGE_PENDING_EQUALITY_CARDINALITY = 100000000;
	static constexpr double SMALL_SIDE_RATIO = 1000;
	if (!has_cardinality_preserving_join) {
		return 0;
	}

	double cost = 0;
	unordered_set<string> seen_inside_sets;
	for (auto &filter : query_graph_manager.GetFilterBindings()) {
		if (!JoinOrderUtil::IsEquivalenceJoinPredicate(*filter) || !filter->left_set || !filter->right_set) {
			continue;
		}
		if (JoinRelationSet::IsSubset(combination, filter->set.get())) {
			continue;
		}

		auto left_inside = Contains(combination, filter->left_set);
		auto right_inside = Contains(combination, filter->right_set);
		if (left_inside == right_inside) {
			continue;
		}

		auto inside = left_inside ? filter->left_set : filter->right_set;
		auto outside = left_inside ? filter->right_set : filter->left_set;
		auto inside_card = cardinality_estimator.EstimateCardinalityWithSet<double>(*inside);
		auto outside_card = cardinality_estimator.EstimateCardinalityWithSet<double>(*outside);
		if (outside_card <= 0 || inside_card < LARGE_PENDING_EQUALITY_CARDINALITY ||
		    inside_card <= outside_card * SMALL_SIDE_RATIO) {
			continue;
		}

		auto inside_key = inside->ToString();
		if (!seen_inside_sets.insert(inside_key).second) {
			continue;
		}
		cost += inside_card;
	}
	return cost;
}

// Currently cost of a join mostly factors in the cardinalities.
// LEFT joins need an explicit RHS input component because their output cardinality preserves the LHS,
// which otherwise makes early LEFT joins over large RHS inputs look almost free.
double CostModel::ComputeCost(DPJoinNode &left, DPJoinNode &right,
                              const vector<reference<NeighborInfo>> &possible_connections) {
	auto &combination = query_graph_manager.set_manager.Union(left.set, right.set);
	auto join_card = cardinality_estimator.EstimateCardinalityWithSet<double>(combination);
	auto join_cost = join_card + GetLeftJoinInputCost(cardinality_estimator, possible_connections) +
	                 GetPendingSmallSidePenalty(query_graph_manager, cardinality_estimator, combination,
	                                            has_cardinality_preserving_join);
	return join_cost + left.cost + right.cost;
}

} // namespace duckdb

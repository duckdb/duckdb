#include "duckdb/optimizer/join_order/join_node.hpp"
#include "duckdb/optimizer/join_order/join_order_optimizer.hpp"
#include "duckdb/optimizer/join_order/cost_model.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"

namespace duckdb {

struct ImbalancedJoinHeuristics {
	static constexpr idx_t LARGE_INPUT_THRESHOLD = 1000000;
	static constexpr idx_t TINY_INPUT_LOWER_BOUND = 10;
	static constexpr idx_t SMALL_INPUT_CONTEXT_THRESHOLD = 100;
	static constexpr idx_t SMALL_INPUT_THRESHOLD = 10000;
	static constexpr double INPUT_RATIO_THRESHOLD = 100;
};

static bool HasEqualityFilter(NeighborInfo &connection) {
	for (auto &filter : connection.filters) {
		if (filter->filter && filter->filter->GetExpressionType() == ExpressionType::COMPARE_EQUAL &&
		    BoundComparisonExpression::IsComparison(*filter->filter)) {
			return true;
		}
	}
	return false;
}

static bool HasEqualityConnection(const vector<reference<NeighborInfo>> &connections) {
	for (auto &connection : connections) {
		if (HasEqualityFilter(connection.get())) {
			return true;
		}
	}
	return false;
}

static bool HasOtherEqualityConnection(const QueryGraphEdges &edges, JoinRelationSet &node, JoinRelationSet &other) {
	bool has_connection = false;
	edges.EnumerateNeighbors(node, [&](NeighborInfo &connection) {
		if (!JoinRelationSet::IsSubset(other, *connection.neighbor) && HasEqualityFilter(connection)) {
			has_connection = true;
			return true;
		}
		return false;
	});
	return has_connection;
}

CostModel::CostModel(QueryGraphManager &query_graph_manager)
    : query_graph_manager(query_graph_manager), cardinality_estimator() {
}

// Currently cost of a join only factors in the cardinalities.
// If join types and join algorithms are to be considered, they should be added here.
double CostModel::ComputeCost(DPJoinNode &left, DPJoinNode &right) {
	auto &combination = query_graph_manager.set_manager.Union(left.set, right.set);
	auto join_card = cardinality_estimator.EstimateCardinalityWithSet<double>(combination);
	auto join_cost = join_card;
	if (left.set.count == 1 && right.set.count == 1) {
		auto &edges = query_graph_manager.GetQueryGraphEdges();
		auto connections = edges.GetConnections(left.set, right.set);
		auto min_card = MinValue(left.cardinality, right.cardinality);
		auto max_card = MaxValue(left.cardinality, right.cardinality);
		auto has_other_connection = HasOtherEqualityConnection(edges, left.set, right.set) &&
		                            HasOtherEqualityConnection(edges, right.set, left.set);
		auto small_side_is_tiny = min_card >= ImbalancedJoinHeuristics::TINY_INPUT_LOWER_BOUND &&
		                          min_card < ImbalancedJoinHeuristics::SMALL_INPUT_CONTEXT_THRESHOLD;
		auto small_side_has_context =
		    min_card >= ImbalancedJoinHeuristics::SMALL_INPUT_CONTEXT_THRESHOLD && has_other_connection;
		if (HasEqualityConnection(connections) && (small_side_is_tiny || small_side_has_context) &&
		    min_card <= ImbalancedJoinHeuristics::SMALL_INPUT_THRESHOLD &&
		    max_card > ImbalancedJoinHeuristics::LARGE_INPUT_THRESHOLD &&
		    static_cast<double>(max_card) / static_cast<double>(min_card) >
		        ImbalancedJoinHeuristics::INPUT_RATIO_THRESHOLD) {
			join_cost = MinValue(join_cost, static_cast<double>(min_card));
		}
	}
	return join_cost + left.cost + right.cost;
}

} // namespace duckdb

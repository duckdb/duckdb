#include "duckdb/optimizer/join_order/join_node.hpp"
#include "duckdb/optimizer/join_order/join_order_optimizer.hpp"
#include "duckdb/optimizer/join_order/cost_model.hpp"

namespace duckdb {

CostModel::CostModel(QueryGraphManager &query_graph_manager)
    : query_graph_manager(query_graph_manager), cardinality_estimator() {
}

double CostModel::ComputeCost(JoinNode &left, JoinNode &right, JoinType join_type) {
	// TODO: need filter info here (like join type).
	cardinality_estimator.PrintRelationToTdomInfo();
	auto &combination = query_graph_manager.set_manager.Union(left.set, right.set);
	auto join_card = cardinality_estimator.EstimateCardinalityWithSet<double>(combination);
	auto join_cost = join_card;
	//	if (join_type != JoinType::INNER) {
	//		join_card = cardinality_estimator.EstimateCardinalityWithSet<double>(left.set);
	//		join_cost = join_card * RelationStatisticsHelper::DEFAULT_SELECTIVITY;
	//	}

	return join_cost + left.cost + right.cost;
}

} // namespace duckdb

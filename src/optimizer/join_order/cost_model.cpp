#include "duckdb/optimizer/join_order/join_node.hpp"
#include "duckdb/optimizer/join_order/join_order_optimizer.hpp"
#include <cmath>

namespace duckdb {

CostModel::CostModel(QueryGraphManager &query_graph_manager) : query_graph_manager(query_graph_manager), cardinality_estimator() {
}


void CostModel::InitCostModel() {

}

idx_t CostModel::ComputeCost(JoinRelationSet &left, JoinRelationSet &right) {
	auto &combination = query_graph_manager.set_manager.Union(left, right);
	auto card = cardinality_estimator.EstimateCardinalityWithSet(combination);
	return card + GetCost(left) + GetCost(right);
}

idx_t CostModel::GetCost(JoinRelationSet &set) {
	if (relation_set_2_cost.find(&set) == relation_set_2_cost.end()) {
		return NumericLimits<idx_t>::Maximum();
	}
	return relation_set_2_cost[&set];
}

idx_t CostModel::GetLeafCost(JoinRelationSet &set) {
	D_ASSERT(set.count == 1);
	return cardinality_estimator.EstimateCardinalityWithSet(set);
}

void CostModel::AddRelationSetCost(JoinRelationSet &set, idx_t cost) {
	relation_set_2_cost[&set] = cost;
}

}

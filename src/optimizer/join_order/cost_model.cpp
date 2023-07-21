#include "duckdb/optimizer/join_order/join_node.hpp"
#include "duckdb/optimizer/join_order/join_order_optimizer.hpp"
#include <cmath>

namespace duckdb {

CostModel::CostModel(QueryGraphManager &query_graph_manager)
    : query_graph_manager(query_graph_manager), cardinality_estimator() {
}

idx_t CostModel::ComputeCost(optional_ptr<JoinNode> left, optional_ptr<JoinNode> right) {
	auto combination = query_graph_manager.set_manager.Union(left->set, right->set);
	auto card = cardinality_estimator.EstimateCardinalityWithSet(*combination);
	return card + left->cost + right->cost;
}

} // namespace duckdb

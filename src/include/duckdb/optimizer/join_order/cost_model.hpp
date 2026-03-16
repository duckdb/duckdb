//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/join_order/cost_model.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/optimizer/join_order/join_node.hpp"
#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/optimizer/join_order/cardinality_estimator.hpp"

namespace duckdb {

class QueryGraphManager;

class CostModel {
public:
	explicit CostModel(QueryGraphManager &query_graph_manager, CardinalityEstimator &cardinality_estimator);

private:
	//! query graph storing relation manager information
	QueryGraphManager &query_graph_manager;

public:
	//! Compute cost of a join relation set
	double ComputeCost(DPJoinNode &left, DPJoinNode &right);
	CardinalityEstimator &GetCardinalityEstimator();

private:
	CardinalityEstimator &cardinality_estimator;
};

} // namespace duckdb

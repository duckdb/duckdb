//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/join_order/cost_model.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/optimizer/join_order/join_node.hpp"
#include "duckdb/optimizer/join_order/cardinality_estimator.hpp"

namespace duckdb {

class QueryGraphManager;

class CostModel {
public:
	CostModel(QueryGraphManager &query_graph_manager);

private:
	//! query graph storing relation manager information
	QueryGraphManager &query_graph_manager;

public:
	void InitCostModel();

	//! Compute cost of a join relation set
	double ComputeCost(JoinNode &left, JoinNode &right);

	//! Cardinality Estimator used to calculate cost
	CardinalityEstimator cardinality_estimator;

private:
};

} // namespace duckdb

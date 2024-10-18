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
	explicit CostModel(QueryGraphManager &query_graph_manager);

private:
	//! query graph storing relation manager information
	QueryGraphManager &query_graph_manager;

public:
	void InitCostModel();

	//! Compute cost of a join relation set
	double ComputeCost(DPJoinNode &left, DPJoinNode &right);

	//! Cardinality Estimator used to calculate cost
	CardinalityEstimator cardinality_estimator;

private:
};

} // namespace duckdb

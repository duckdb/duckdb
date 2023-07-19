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

class CostModel {
public:
	explicit CostModel(QueryGraphManager query_graph_manager) : query_graph_manager(query_graph_manager) {
	}

private:
	//! query graph storing relation manager information
	QueryGraphManager query_graph_manager;
	//! Cardinality Estimator used to calculate cost
	CardinalityEstimator cardinality_estimator;
	//! map of join relation set to cost
	unordered_map<JoinRelationSet *, idx_t> cost;

public:

	static idx_t ComputeCost(JoinNode &left, JoinNode &right);

private:

};

} // namespace duckdb

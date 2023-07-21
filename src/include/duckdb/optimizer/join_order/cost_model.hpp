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
	CostModel(QueryGraphManager &query_graph_manager);

private:
	//! query graph storing relation manager information
	QueryGraphManager &query_graph_manager;

	//! map of join relation set to cost
	unordered_map<JoinRelationSet *, idx_t> relation_set_2_cost;

public:
	void InitCostModel();

	//! Compute cost of a join relation set
	idx_t ComputeCost(JoinRelationSet &left, JoinRelationSet &right);
	//! Return lowest predicted cost of calculating a join relation set calculated up until this point
	idx_t GetCost(JoinRelationSet &set);
	//! If the plan enumerator needs to approximate the join order, it needs the cardinality of leaf nodes
	//! Normally a leaf has a cost of 0, so we include this function to return the desirec cardinality.
	idx_t GetLeafCost(JoinRelationSet &set);
	void AddRelationSetCost(JoinRelationSet &set, idx_t cost);

	//! Cardinality Estimator used to calculate cost
	CardinalityEstimator cardinality_estimator;

private:

};

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/join_order/join_order_linearizer.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/join_order/join_order_rank.hpp"

namespace duckdb {

class CardinalityEstimator;
class QueryGraphManager;

//! Minimum-selectivity spanning tree followed by rooted IK/KBZ chain normalization.
class JoinOrderLinearizer {
public:
	JoinOrderLinearizer(QueryGraphManager &graph, CardinalityEstimator &estimator);
	bool BuildTree();
	RankedJoinOrder GetOrder(RelationIndex root);
	//! Compare every root using the tree proxy before choosing one order for interval DP.
	vector<RelationIndex> GetBestOrder();

private:
	struct Block {
		Block(RelationIndex relation, double multiplier);

		vector<RelationIndex> relations;
		JoinOrderRank rank;
	};

	struct Edge {
		idx_t left;
		idx_t right;
		double log_selectivity;
	};

private:
	void Normalize(vector<Block> &chain);

private:
	QueryGraphManager &graph;
	CardinalityEstimator &estimator;
	vector<double> cardinalities;
	vector<Edge> tree;
};

} // namespace duckdb

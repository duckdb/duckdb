//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/join_order/join_order_linearizer.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/join_order/cardinality_value.hpp"
#include "duckdb/optimizer/join_order/relation_index.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

class CardinalityEstimator;
class QueryGraphManager;

//! ASI ranking proxy (q, w), stored logarithmically. This is not the plan cost comparator.
struct JoinOrderRank {
	explicit JoinOrderRank(double log_multiplier);
	void Append(const JoinOrderRank &other);
	bool operator<(const JoinOrderRank &other) const;

	double log_multiplier;
	double log_work;
};

struct RankedJoinOrder {
	vector<RelationIndex> relations;
	double log_cost;
};

//! Minimum-selectivity spanning tree followed by rooted IK/KBZ chain normalization.
class JoinOrderLinearizer {
public:
	JoinOrderLinearizer(QueryGraphManager &graph, CardinalityEstimator &estimator);
	bool BuildTree();
	RankedJoinOrder GetOrder(RelationIndex root);
	//! Compare every root using the tree proxy before choosing one order for interval DP.
	vector<RelationIndex> GetBestOrder();

private:
	struct Edge {
		idx_t left;
		idx_t right;
		double log_selectivity;
	};

	QueryGraphManager &graph;
	CardinalityEstimator &estimator;
	vector<double> cardinalities;
	vector<Edge> tree;
};

} // namespace duckdb

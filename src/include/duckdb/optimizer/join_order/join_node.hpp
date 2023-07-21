//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/join_order/join_node.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/join_order/join_relation.hpp"
#include "duckdb/optimizer/join_order/query_graph.hpp"
#include "duckdb/storage/statistics/distinct_statistics.hpp"

namespace duckdb {

class JoinOrderOptimizer;

class JoinNode {
public:
	//! Represents a node in the join plan
	JoinRelationSet &set;
	optional_ptr<NeighborInfo> info;
	//! If the JoinNode is a base table, then base_cardinality is the cardinality before filters
	//! estimated_props.cardinality will be the cardinality after filters. With no filters, the two are equal
	optional_ptr<JoinNode> left;
	optional_ptr<JoinNode> right;

	//! Create an intermediate node in the join tree. base_cardinality = estimated_props.cardinality
	JoinNode(JoinRelationSet &set, optional_ptr<NeighborInfo> info, optional_ptr<JoinNode> left, optional_ptr<JoinNode> right);

	//! Create a leaf node in the join tree
	//! set cost to 0 for leaf nodes
	//! cost will be the cost to *produce* an intermediate table
	JoinNode(JoinRelationSet &set);



	bool operator==(const JoinNode &other) {
		return other.set.ToString().compare(set.ToString()) == 0;
	}

private:

public:
	void PrintJoinNode();
	string ToString();
};

} // namespace duckdb

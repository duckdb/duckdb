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

namespace duckdb {

struct NeighborInfo;

class DPJoinNode {
public:
	//! Represents a node in the join plan
	JoinRelationSet &set;
	//! information on how left and right are connected
	optional_ptr<NeighborInfo> info;
	bool is_leaf;
	//! left and right plans
	JoinRelationSet &left_set;
	JoinRelationSet &right_set;

	//! The cost of the join node. The cost is stored here so that the cost of
	//! a join node stays in sync with how the join node is constructed. Storing the cost in an unordered_set
	//! in the cost model is error prone. If the plan enumerator join node is updated and not the cost model
	//! the whole Join Order Optimizer can start exhibiting undesired behavior.
	double cost;
	//! used only to populate logical operators with estimated cardinalities after the best join plan has been found.
	idx_t cardinality;

	//! Create an intermediate node in the join tree. base_cardinality = estimated_props.cardinality
	DPJoinNode(JoinRelationSet &set, optional_ptr<NeighborInfo> info, JoinRelationSet &left, JoinRelationSet &right,
	           double cost);

	//! Create a leaf node in the join tree
	//! set cost to 0 for leaf nodes
	//! cost will be the cost to *produce* an intermediate table
	explicit DPJoinNode(JoinRelationSet &set);
};

} // namespace duckdb

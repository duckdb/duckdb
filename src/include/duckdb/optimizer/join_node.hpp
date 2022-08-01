//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/join_node.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/optimizer/join_order/query_graph.hpp"
#include "duckdb/optimizer/join_order/join_relation.hpp"
#include "duckdb/parser/expression_map.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/storage/statistics/distinct_statistics.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/optimizer/estimated_properties.hpp"

namespace duckdb {

class JoinOrderOptimizer;

class JoinNode {
public:
	//! Represents a node in the join plan
	JoinRelationSet *set;
	NeighborInfo *info;
	//! If the JoinNode is a base table, then base_cardinality is the cardinality before filters
	//! estimated_props.cardinality will be the cardinality after filters. With no filters, the two are equal
	bool has_filter;
	JoinNode *left;
	JoinNode *right;

	unique_ptr<EstimatedProperties> estimated_props;

	//! Create a leaf node in the join tree
	//! set cost to 0 for leaf nodes
	//! cost will be the cost to *produce* an intermediate table
	JoinNode(JoinRelationSet *set, const double base_cardinality)
	    : set(set), info(nullptr), has_filter(false), left(nullptr), right(nullptr),
	      base_cardinality(base_cardinality) {
		estimated_props = make_unique<EstimatedProperties>(base_cardinality, 0);
	}
	//! Create an intermediate node in the join tree. base_cardinality = estimated_props.cardinality
	JoinNode(JoinRelationSet *set, NeighborInfo *info, JoinNode *left, JoinNode *right, const double base_cardinality,
	         double cost)
	    : set(set), info(info), has_filter(false), left(left), right(right), base_cardinality(base_cardinality) {
		estimated_props = make_unique<EstimatedProperties>(base_cardinality, cost);
	}

private:
	double base_cardinality;

public:
	double GetCardinality() const;
	double GetCost();
	double GetBaseTableCardinality();
	void SetBaseTableCardinality(double base_card);
	void SetEstimatedCardinality(double estimated_card);
	void PrintJoinNode();
	string ToString();
};

} // namespace duckdb

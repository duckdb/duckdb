//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/join_order/query_graph.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/optimizer/join_order/join_relation.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/planner/column_binding.hpp"

#include <functional>

namespace duckdb {
class Expression;
class LogicalOperator;

struct FilterInfo {
	idx_t filter_index;
	JoinRelationSet *left_set = nullptr;
	JoinRelationSet *right_set = nullptr;
	ColumnBinding left_binding;
	ColumnBinding right_binding;
	JoinRelationSet *set = nullptr;
};

struct FilterNode {
	vector<FilterInfo *> filters;
	unordered_map<idx_t, unique_ptr<FilterNode>> children;
};

struct NeighborInfo {
	JoinRelationSet *neighbor;
	vector<FilterInfo *> filters;
};

//! The QueryGraph contains edges between relations and allows edges to be created/queried
class QueryGraph {
public:
	//! Contains a node with info about neighboring relations and child edge infos
	struct QueryEdge {
		vector<unique_ptr<NeighborInfo>> neighbors;
		unordered_map<idx_t, unique_ptr<QueryEdge>> children;
	};

public:
	string ToString() const;
	void Print();

	//! Create an edge in the edge_set
	void CreateEdge(JoinRelationSet *left, JoinRelationSet *right, FilterInfo *info);
	//! Returns a connection if there is an edge that connects these two sets, or nullptr otherwise
	vector<NeighborInfo *> GetConnections(JoinRelationSet *node, JoinRelationSet *other);
	//! Enumerate the neighbors of a specific node that do not belong to any of the exclusion_set. Note that if a
	//! neighbor has multiple nodes, this function will return the lowest entry in that set.
	vector<idx_t> GetNeighbors(JoinRelationSet *node, unordered_set<idx_t> &exclusion_set);
	//! Enumerate all neighbors of a given JoinRelationSet node
	void EnumerateNeighbors(JoinRelationSet *node, const std::function<bool(NeighborInfo *)> &callback);

private:
	//! Get the QueryEdge of a specific node
	QueryEdge *GetQueryEdge(JoinRelationSet *left);

	QueryEdge root;
};

} // namespace duckdb

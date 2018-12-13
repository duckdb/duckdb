//===----------------------------------------------------------------------===//
//                         DuckDB
//
// optimizer/join_order/edge.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"
#include "optimizer/join_order/relation.hpp"
#include <unordered_set>

namespace duckdb {
class Expression;
class LogicalOperator;

struct FilterInfo {
	Expression *filter;
	LogicalOperator *parent;
	RelationSet *left_set;
	RelationSet *right_set;
};

struct FilterNode {
	vector<FilterInfo*> filters;
	unordered_map<size_t, unique_ptr<FilterNode>> children;
};

struct NeighborInfo {
	RelationSet* neighbor;
	vector<FilterInfo*> filters;
};

//! The QueryGraph contains edges between relations and allows edges to be created/queried
class QueryGraph {
public:
	//! Contains a node with info about neighboring relations and child edge infos
	struct QueryEdge {
		vector<unique_ptr<NeighborInfo>> neighbors;
		unordered_map<size_t, unique_ptr<QueryEdge>> children;
	};
public:
	//! Create an edge in the edge_set
	void CreateEdge(RelationSet *left, RelationSet *right, FilterInfo* info);
	//! Returns a connection if there is an edge that connects these two sets, or nullptr otherwise
	NeighborInfo* GetConnection(RelationSet *node, RelationSet *other);
	//! Enumerate the neighbors of a specific node that do not belong to any of the exclusion_set. Note that if a neighbor has multiple nodes, this function will return the lowest entry in that set.
	vector<size_t> GetNeighbors(RelationSet *node, std::unordered_set<size_t> &exclusion_set);
	//! Enumerate all neighbors of a given RelationSet node
	void EnumerateNeighbors(RelationSet *node, std::function<bool(NeighborInfo*)> callback);
private:
	//! Get the QueryEdge of a specific node
	QueryEdge* GetQueryEdge(RelationSet *left);

	QueryEdge root;
};

}
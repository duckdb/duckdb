//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/join_order/query_graph.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/optimizer/join_order/join_relation.hpp"
#include "duckdb/optimizer/join_order/join_node.hpp"
#include "duckdb/optimizer/join_order/relation_manager.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/planner/column_binding.hpp"

#include <functional>

namespace duckdb {

struct FilterInfo;

struct NeighborInfo {
	NeighborInfo(optional_ptr<JoinRelationSet> neighbor) : neighbor(neighbor) {
	}

	optional_ptr<JoinRelationSet> neighbor;
	vector<optional_ptr<FilterInfo>> filters;
};

//! The QueryGraph contains edges between relations and allows edges to be created/queried
class QueryGraphEdges {
public:
	//! Contains a node with info about neighboring relations and child edge infos
	struct QueryEdge {
		vector<unique_ptr<NeighborInfo>> neighbors;
		unordered_map<idx_t, unique_ptr<QueryEdge>> children;
	};

public:
	string ToString() const;
	void Print();

	//! Returns a connection if there is an edge that connects these two sets, or nullptr otherwise
	const vector<reference<NeighborInfo>> GetConnections(JoinRelationSet &node, JoinRelationSet &other) const;
	//! Enumerate the neighbors of a specific node that do not belong to any of the exclusion_set. Note that if a
	//! neighbor has multiple nodes, this function will return the lowest entry in that set.
	const vector<idx_t> GetNeighbors(JoinRelationSet &node, unordered_set<idx_t> &exclusion_set) const;

	//! Enumerate all neighbors of a given JoinRelationSet node
	void EnumerateNeighbors(JoinRelationSet &node, const std::function<bool(NeighborInfo &)> &callback) const;
	//! Create an edge in the edge_set
	void CreateEdge(JoinRelationSet &left, JoinRelationSet &right, optional_ptr<FilterInfo> info);

private:
	//! Get the QueryEdge of a specific node
	optional_ptr<QueryEdge> GetQueryEdge(JoinRelationSet &left);

	void EnumerateNeighborsDFS(JoinRelationSet &node, reference<QueryEdge> info, idx_t index,
	                           const std::function<bool(NeighborInfo &)> &callback) const;

	QueryEdge root;
};

} // namespace duckdb

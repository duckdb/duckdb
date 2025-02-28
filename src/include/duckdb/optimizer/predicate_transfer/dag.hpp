#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/optimizer/predicate_transfer/bloom_filter/bloom_filter.hpp"

namespace duckdb {
class GraphEdge {
public:
	explicit GraphEdge(idx_t destination) : destination(destination) {
	}

	idx_t destination;

	vector<Expression *> filters;
	vector<shared_ptr<BlockedBloomFilter>> bloom_filters;
};

struct Edges {
	vector<unique_ptr<GraphEdge>> in;
	vector<unique_ptr<GraphEdge>> out;
};

class GraphNode {
public:
	GraphNode(idx_t id, idx_t est_cardinality, bool is_root)
	    : id(id), is_root(is_root), priority(-1), est_cardinality(est_cardinality) {
	}

	idx_t id;
	bool is_root;
	int32_t priority;
	idx_t est_cardinality;

	//! Predicate Transfer has two stages, the transfer graph is different because of the existence of LEFT JOIN, RIGHT
	//! JOIN, etc.
	Edges forward_stage_edges;
	Edges backward_stage_edges;

	GraphEdge *Add(idx_t other, bool is_forward, bool is_in_edge);
	GraphEdge *Add(idx_t other, Expression *expression, bool is_forward, bool is_in_edge);
	GraphEdge *Add(idx_t other, const shared_ptr<BlockedBloomFilter> &bloom_filter, bool is_forward, bool is_in_edge);
};

using TransferGraph = unordered_map<idx_t, unique_ptr<GraphNode>>;
} // namespace duckdb
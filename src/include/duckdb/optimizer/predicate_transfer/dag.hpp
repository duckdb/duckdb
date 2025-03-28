//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/predicate_transfer/dag.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/optimizer/predicate_transfer/bloom_filter/bloom_filter.hpp"

namespace duckdb {

struct FilterPlan {
	vector<unique_ptr<Expression>> build;
	vector<unique_ptr<Expression>> apply;

	vector<idx_t> bound_cols_build;
	vector<idx_t> bound_cols_apply;

	void Serialize(Serializer &serializer) const;
	static unique_ptr<FilterPlan> Deserialize(Deserializer &deserializer);

	bool operator==(const FilterPlan &other) const;
};

class GraphEdge {
public:
	explicit GraphEdge(idx_t destination) : destination(destination) {
	}

	idx_t destination;

	vector<Expression *> conditions;
	vector<shared_ptr<FilterPlan>> filter_plan;
};

struct Edges {
	vector<unique_ptr<GraphEdge>> in;
	vector<unique_ptr<GraphEdge>> out;
};

class GraphNode {
public:
	GraphNode(idx_t id, int32_t priority) : id(id), priority(priority) {
	}

	idx_t id;
	int32_t priority;

	//! Predicate Transfer has two stages. The transfer graph is different because of the existence of LEFT JOIN, RIGHT
	//! JOIN, etc.
	Edges forward_stage_edges;
	Edges backward_stage_edges;

public:
	GraphEdge *Add(idx_t other, bool is_forward, bool is_in_edge);
	GraphEdge *Add(idx_t other, Expression *expression, bool is_forward, bool is_in_edge);
	GraphEdge *Add(idx_t other, const shared_ptr<FilterPlan> &filter_plan, bool is_forward, bool is_in_edge);
};

using TransferGraph = unordered_map<idx_t, unique_ptr<GraphNode>>;
} // namespace duckdb

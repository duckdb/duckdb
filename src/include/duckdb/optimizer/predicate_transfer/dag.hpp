#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/optimizer/predicate_transfer/bloom_filter/bloom_filter.hpp"

namespace duckdb {
class DAGEdge;

class DAGNode {
public:
	DAGNode(idx_t id, idx_t est_cardinality, bool is_root)
	    : id(id), is_root(is_root), est_cardinality(est_cardinality) {
	}

	idx_t id;
	bool is_root;
	int priority = -1;
	idx_t est_cardinality;

	vector<unique_ptr<DAGEdge>> forward_in_;
	vector<unique_ptr<DAGEdge>> backward_in_;
	vector<unique_ptr<DAGEdge>> forward_out_;
	vector<unique_ptr<DAGEdge>> backward_out_;

	void AddIn(idx_t from, Expression *filter, bool forward);
	void AddIn(idx_t from, shared_ptr<BlockedBloomFilter> bloom_filter, bool forward);
	void AddOut(idx_t to, Expression *filter, bool forward);
	void AddOut(idx_t to, shared_ptr<BlockedBloomFilter> bloom_filter, bool forward);
};

class DAGEdge {
public:
	explicit DAGEdge(idx_t id) : dest_(id) {
	}

	void Push(Expression *filter) {
		filters.emplace_back(filter);
	}

	void Push(const shared_ptr<BlockedBloomFilter> &bloom_filter) {
		bloom_filters.emplace_back(bloom_filter);
	}

	idx_t GetDest() const {
		return dest_;
	}

	idx_t dest_;
	vector<Expression *> filters;
	vector<shared_ptr<BlockedBloomFilter>> bloom_filters;
};

using DAG = unordered_map<idx_t, unique_ptr<DAGNode>>;
} // namespace duckdb
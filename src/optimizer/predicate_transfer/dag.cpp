#include "duckdb/optimizer/predicate_transfer/dag.hpp"

namespace duckdb {

GraphEdge *GraphNode::Add(idx_t other, bool is_forward, bool is_in_edge) {
	auto &stage = (is_forward ? forward_stage_edges : backward_stage_edges);
	auto &edges = (is_in_edge ? stage.in : stage.out);
	for (auto &edge : edges) {
		if (edge->destination == other) {
			return edge.get();
		}
	}
	edges.emplace_back(make_uniq<GraphEdge>(other));
	return edges.back().get();
}

GraphEdge *GraphNode::Add(idx_t other, Expression *expression, bool is_forward, bool is_in_edge) {
	auto *edge = Add(other, is_forward, is_in_edge);
	edge->filters.push_back(expression);
	return edge;
}

GraphEdge *GraphNode::Add(idx_t other, const shared_ptr<BlockedBloomFilter> &bloom_filter, bool is_forward,
                          bool is_in_edge) {
	auto *edge = Add(other, is_forward, is_in_edge);
	edge->bloom_filters.push_back(bloom_filter);
	return edge;
}
} // namespace duckdb
#include "duckdb/optimizer/predicate_transfer/dag.hpp"

namespace duckdb {
void GraphNode::AddIn(idx_t from, Expression *filter, bool forward) {
	if (forward) {
		for (auto &node : forward_in_) {
			if (node->GetDest() == from) {
				node->Push(filter);
				return;
			}
		}
		auto node = make_uniq<GraphEdge>(from);
		node->Push(filter);
		forward_in_.emplace_back(std::move(node));
	} else {
		for (auto &node : backward_in_) {
			if (node->GetDest() == from) {
				node->Push(filter);
				return;
			}
		}
		auto node = make_uniq<GraphEdge>(from);
		node->Push(filter);
		backward_in_.emplace_back(std::move(node));
	}
	return;
}

void GraphNode::AddIn(idx_t from, shared_ptr<BlockedBloomFilter> bloom_filter, bool forward) {
	if (forward) {
		for (auto &node : forward_in_) {
			if (node->GetDest() == from) {
				node->Push(bloom_filter);
				return;
			}
		}
		auto node = make_uniq<GraphEdge>(from);
		node->Push(bloom_filter);
		forward_in_.emplace_back(std::move(node));
	} else {
		for (auto &node : backward_in_) {
			if (node->GetDest() == from) {
				node->Push(bloom_filter);
				return;
			}
		}
		auto node = make_uniq<GraphEdge>(from);
		node->Push(bloom_filter);
		backward_in_.emplace_back(std::move(node));
	}
}

void GraphNode::AddOut(idx_t to, Expression *filter, bool forward) {
	if (forward) {
		for (auto &node : forward_out_) {
			if (node->GetDest() == to) {
				node->Push(filter);
				return;
			}
		}
		auto node = make_uniq<GraphEdge>(to);
		node->Push(std::move(filter));
		forward_out_.emplace_back(std::move(node));
	} else {
		for (auto &node : backward_out_) {
			if (node->GetDest() == to) {
				node->Push(filter);
				return;
			}
		}
		auto node = make_uniq<GraphEdge>(to);
		node->Push(std::move(filter));
		backward_out_.emplace_back(std::move(node));
	}
}

void GraphNode::AddOut(idx_t to, shared_ptr<BlockedBloomFilter> bloom_filter, bool forward) {
	if (forward) {
		for (auto &node : forward_out_) {
			if (node->GetDest() == to) {
				node->Push(bloom_filter);
				return;
			}
		}
		auto node = make_uniq<GraphEdge>(to);
		node->Push(bloom_filter);
		forward_out_.emplace_back(std::move(node));
	} else {
		for (auto &node : backward_out_) {
			if (node->GetDest() == to) {
				node->Push(bloom_filter);
				return;
			}
		}
		auto node = make_uniq<GraphEdge>(to);
		node->Push(bloom_filter);
		backward_out_.emplace_back(std::move(node));
	}
}
} // namespace duckdb
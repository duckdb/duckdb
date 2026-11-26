#include "duckdb/optimizer/join_order/join_order_linearizer.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/optimizer/join_order/query_graph_manager.hpp"

#include <cmath>
#include <limits>

namespace duckdb {

JoinOrderLinearizer::JoinOrderLinearizer(QueryGraphManager &graph, CardinalityEstimator &estimator)
    : graph(graph), estimator(estimator) {
}

bool JoinOrderLinearizer::BuildTree() {
	for (auto &op : graph.GetJoinOperators()) {
		if (op->type != JoinOrderOperatorType::INNER && op->type != JoinOrderOperatorType::CROSS_PRODUCT) {
			return false;
		}
	}
	auto count = graph.relation_manager.NumRelations();
	vector<CardinalityValue> input_cardinalities;
	input_cardinalities.reserve(count);
	for (idx_t i = 0; i < count; i++) {
		graph.context.InterruptCheck();
		auto &set = graph.set_manager.GetJoinRelation(RelationIndex(i));
		input_cardinalities.push_back(estimator.EstimateCardinality(set));
		cardinalities.push_back(input_cardinalities.back().GetLog());
	}
	vector<Edge> edges;
	bool edges_complete = true;
	graph.GetQueryGraphEdges().EnumerateEdges([&](JoinRelationSet &left, NeighborInfo &edge) {
		graph.context.InterruptCheck();
		auto &right = *edge.neighbor;
		if (left.count != 1 || right.count != 1) {
			edges_complete = false;
			return true;
		}
		auto lhs = left.relations[0].index;
		auto rhs = right.relations[0].index;
		auto &set = graph.set_manager.Union(left, right);
		double selectivity = 0;
		if (std::isfinite(cardinalities[lhs]) && std::isfinite(cardinalities[rhs])) {
			// Divide before taking logs: subtracting large logs can reorder equal-selectivity edges.
			auto input_rows = input_cardinalities[lhs] * input_cardinalities[rhs];
			selectivity = (estimator.EstimateCardinality(set) / input_rows).GetLog();
		}
		edges.push_back({MinValue(lhs, rhs), MaxValue(lhs, rhs), selectivity});
		return false;
	});
	if (!edges_complete) {
		return false;
	}
	vector<idx_t> parent;
	for (idx_t i = 0; i < count; i++) {
		parent.push_back(i);
	}
	auto find_root = [&](idx_t node) {
		while (parent[node] != node) {
			parent[node] = parent[parent[node]];
			node = parent[node];
		}
		return node;
	};
	// Check active ordinary-edge connectivity before constructing the ranking tree. Missing connections must
	// still go through the existing cross-product-capable search, not an arbitrary spanning forest order.
	idx_t components = count;
	for (auto &edge : edges) {
		graph.context.InterruptCheck();
		auto left = find_root(edge.left);
		auto right = find_root(edge.right);
		if (left != right) {
			parent[right] = left;
			components--;
		}
	}
	if (components != 1) {
		return false;
	}
	for (idx_t i = 0; i < count; i++) {
		parent[i] = i;
	}
	std::sort(edges.begin(), edges.end(), [](const Edge &left, const Edge &right) {
		if (left.log_selectivity != right.log_selectivity) {
			return left.log_selectivity < right.log_selectivity;
		}
		return left.left != right.left ? left.left < right.left : left.right < right.right;
	});
	for (auto &edge : edges) {
		graph.context.InterruptCheck();
		auto left = find_root(edge.left);
		auto right = find_root(edge.right);
		if (left != right) {
			tree.push_back(edge);
			parent[right] = left;
		}
	}
	D_ASSERT(tree.size() + 1 == count);
	return true;
}

JoinOrderLinearizer::Block::Block(RelationIndex relation, double multiplier) : relations {relation}, rank(multiplier) {
}

void JoinOrderLinearizer::Normalize(vector<Block> &chain) {
	vector<Block> normalized;
	for (auto &block : chain) {
		graph.context.InterruptCheck();
		normalized.push_back(std::move(block));
		while (normalized.size() > 1 && normalized.back().rank < normalized[normalized.size() - 2].rank) {
			auto &left = normalized[normalized.size() - 2];
			auto &right = normalized.back();
			left.rank.Append(right.rank);
			left.relations.insert(left.relations.end(), right.relations.begin(), right.relations.end());
			normalized.pop_back();
		}
	}
	chain = std::move(normalized);
}

RankedJoinOrder JoinOrderLinearizer::GetOrder(RelationIndex root) {
	auto count = cardinalities.size();
	D_ASSERT(root.index < count);
	vector<vector<pair<idx_t, double>>> adjacency(count);
	for (auto &edge : tree) {
		adjacency[edge.left].emplace_back(edge.right, edge.log_selectivity);
		adjacency[edge.right].emplace_back(edge.left, edge.log_selectivity);
	}
	vector<idx_t> parent(count, DConstants::INVALID_INDEX);
	vector<double> multiplier(count);
	vector<idx_t> preorder {root.index};
	parent[root.index] = root.index;
	multiplier[root.index] = cardinalities[root.index];
	for (idx_t i = 0; i < preorder.size(); i++) {
		auto node = preorder[i];
		for (auto &edge : adjacency[node]) {
			graph.context.InterruptCheck();
			if (parent[edge.first] != DConstants::INVALID_INDEX) {
				continue;
			}
			parent[edge.first] = node;
			multiplier[edge.first] = cardinalities[edge.first] + edge.second;
			preorder.push_back(edge.first);
		}
	}
	D_ASSERT(preorder.size() == count);
	vector<vector<Block>> chains(count);
	for (idx_t i = preorder.size(); i > 0; i--) {
		auto node = preorder[i - 1];
		auto &chain = chains[node];
		chain.emplace_back(RelationIndex(node), multiplier[node]);
		vector<idx_t> children;
		for (auto &edge : adjacency[node]) {
			if (parent[edge.first] == node && edge.first != root.index) {
				Normalize(chains[edge.first]);
				children.push_back(edge.first);
			}
		}
		vector<idx_t> positions(children.size(), 0);
		while (true) {
			idx_t best = DConstants::INVALID_INDEX;
			for (idx_t child = 0; child < children.size(); child++) {
				graph.context.InterruptCheck();
				auto &child_chain = chains[children[child]];
				if (positions[child] == child_chain.size()) {
					continue;
				}
				if (best == DConstants::INVALID_INDEX) {
					best = child;
					continue;
				}
				auto &candidate = child_chain[positions[child]];
				auto &incumbent = chains[children[best]][positions[best]];
				if (candidate.rank < incumbent.rank ||
				    (!(incumbent.rank < candidate.rank) && candidate.relations[0] < incumbent.relations[0])) {
					best = child;
				}
			}
			if (best == DConstants::INVALID_INDEX) {
				break;
			}
			chain.push_back(std::move(chains[children[best]][positions[best]++]));
		}
	}
	RankedJoinOrder result;
	JoinOrderRank cost(multiplier[root.index]);
	// Leaves have no join cost; only the subsequent compound blocks contribute work.
	cost.log_work = -std::numeric_limits<double>::infinity();
	for (idx_t i = 1; i < chains[root.index].size(); i++) {
		cost.Append(chains[root.index][i].rank);
	}
	result.log_cost = cost.log_work;
	for (auto &block : chains[root.index]) {
		result.relations.insert(result.relations.end(), block.relations.begin(), block.relations.end());
	}
	return result;
}

vector<RelationIndex> JoinOrderLinearizer::GetBestOrder() {
	D_ASSERT(!cardinalities.empty());
	auto best = GetOrder(RelationIndex(0));
	for (idx_t root = 1; root < cardinalities.size(); root++) {
		auto candidate = GetOrder(RelationIndex(root));
		if (candidate.log_cost < best.log_cost) {
			best = std::move(candidate);
		}
	}
	return std::move(best.relations);
}

} // namespace duckdb

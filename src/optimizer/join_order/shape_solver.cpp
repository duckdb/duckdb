#include "duckdb/optimizer/join_order/shape_solver.hpp"

#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/bit_utils.hpp"

#include <cmath>

namespace duckdb {

static bool UsableValue(double value) {
	return std::isfinite(value) && value >= 0;
}

ShapeTree::ShapeTree(idx_t relation_p) : is_leaf(true), relation(relation_p) {
}

ShapeTree::ShapeTree(unique_ptr<ShapeTree> left_p, unique_ptr<ShapeTree> right_p)
    : is_leaf(false), relation(DConstants::INVALID_INDEX), left(std::move(left_p)), right(std::move(right_p)) {
}

static void CollectLeaves(const ShapeTree &tree, vector<idx_t> &result) {
	if (tree.is_leaf) {
		result.push_back(tree.relation);
		return;
	}
	CollectLeaves(*tree.left, result);
	CollectLeaves(*tree.right, result);
}

vector<idx_t> ShapeTree::Leaves() const {
	vector<idx_t> result;
	CollectLeaves(*this, result);
	return result;
}

ShapeGraph::ShapeGraph(vector<double> weights_p, vector<ShapeEdge> edges_p)
    : weights(std::move(weights_p)), edges(std::move(edges_p)), usable(true) {
	// Bounded by the neighbour bitmask, not by what a given solver can afford to search.
	const auto count = weights.size();
	if (count < 2 || count > MAX_SHAPE_RELATIONS) {
		usable = false;
		return;
	}
	for (auto weight : weights) {
		if (!UsableValue(weight)) {
			usable = false;
			return;
		}
	}

	selectivity.resize(count, vector<double>(count, 1));
	neighbors.resize(count, 0);
	for (auto &edge : edges) {
		if (edge.left == edge.right || edge.left >= count || edge.right >= count || !UsableValue(edge.selectivity)) {
			usable = false;
			return;
		}
		const auto low = MinValue(edge.left, edge.right);
		const auto high = MaxValue(edge.left, edge.right);
		// Several predicates over one pair are a single edge whose selectivities are already combined.
		if (neighbors[low] & (ShapeSubset(1) << high)) {
			usable = false;
			return;
		}
		selectivity[low][high] = edge.selectivity;
		neighbors[low] |= ShapeSubset(1) << high;
		neighbors[high] |= ShapeSubset(1) << low;
	}
}

double ShapeGraph::Selectivity(idx_t left, idx_t right) const {
	return selectivity[MinValue(left, right)][MaxValue(left, right)];
}

double ShapeGraph::SubsetSize(ShapeSubset subset) const {
	double total = 1;
	for (idx_t relation = 0; relation < weights.size(); relation++) {
		if (subset & (ShapeSubset(1) << relation)) {
			total *= weights[relation];
		}
	}
	for (idx_t left = 0; left < selectivity.size(); left++) {
		if (!(subset & (ShapeSubset(1) << left))) {
			continue;
		}
		for (idx_t right = left + 1; right < selectivity.size(); right++) {
			if (subset & (ShapeSubset(1) << right)) {
				total *= selectivity[left][right];
			}
		}
	}
	return total;
}

bool ShapeGraph::IsConnected(ShapeSubset subset) const {
	if (subset == 0) {
		return false;
	}
	ShapeSubset reached = subset & (~subset + 1);
	auto frontier = reached;
	while (frontier != 0) {
		ShapeSubset adjacent = 0;
		auto remaining = frontier;
		while (remaining != 0) {
			const auto relation = CountZeros<ShapeSubset>::Trailing(remaining);
			adjacent |= neighbors[relation] & subset;
			remaining &= remaining - 1;
		}
		frontier = adjacent & ~reached;
		reached |= frontier;
	}
	return reached == subset;
}

bool ShapeGraph::Crosses(ShapeSubset left, ShapeSubset right) const {
	auto remaining = left;
	while (remaining != 0) {
		const auto relation = CountZeros<ShapeSubset>::Trailing(remaining);
		if (neighbors[relation] & right) {
			return true;
		}
		remaining &= remaining - 1;
	}
	return false;
}

vector<vector<idx_t>> BuildShapeAdjacency(idx_t relation_count, const vector<ShapeEdge> &edges) {
	vector<vector<idx_t>> adjacency(relation_count);
	for (auto &edge : edges) {
		if (edge.left >= relation_count || edge.right >= relation_count) {
			return vector<vector<idx_t>>();
		}
		adjacency[edge.left].push_back(edge.right);
		adjacency[edge.right].push_back(edge.left);
	}
	for (auto &neighbors : adjacency) {
		std::sort(neighbors.begin(), neighbors.end());
	}
	return adjacency;
}

bool IsShapeConnected(const vector<vector<idx_t>> &adjacency) {
	if (adjacency.empty()) {
		return false;
	}
	vector<bool> seen(adjacency.size(), false);
	vector<idx_t> stack {0};
	seen[0] = true;
	idx_t reached = 1;
	while (!stack.empty()) {
		const auto relation = stack.back();
		stack.pop_back();
		for (auto neighbor : adjacency[relation]) {
			if (!seen[neighbor]) {
				seen[neighbor] = true;
				reached++;
				stack.push_back(neighbor);
			}
		}
	}
	return reached == adjacency.size();
}

} // namespace duckdb

#include "duckdb/optimizer/join_order/shape_solver.hpp"

#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/bit_utils.hpp"

#include <cmath>
#include <limits>

namespace duckdb {

namespace {

constexpr double INFINITE_COST = std::numeric_limits<double>::infinity();

ShapeSubset Bit(idx_t relation) {
	return ShapeSubset(1) << relation;
}

//! Walk the quotient graph as a path, returning the spine in chain order.
bool SpinePath(const vector<vector<idx_t>> &quotient, const vector<bool> &is_link, idx_t expected,
               vector<idx_t> &result) {
	vector<idx_t> members;
	for (idx_t relation = 0; relation < quotient.size(); relation++) {
		if (!is_link[relation]) {
			members.push_back(relation);
		}
	}
	if (members.size() != expected) {
		return false;
	}
	// A branch fails the degree check, a cycle of diamonds fails the ends check below.
	for (auto relation : members) {
		if (quotient[relation].size() > 2) {
			return false;
		}
	}

	vector<idx_t> ends;
	for (auto relation : members) {
		if (quotient[relation].size() == 1) {
			ends.push_back(relation);
		}
	}
	if (ends.size() != 2) {
		return false;
	}
	std::sort(ends.begin(), ends.end());

	// Start from the lower end so the reported order is deterministic.
	auto current = ends[0];
	bool has_previous = false;
	idx_t previous = 0;
	while (true) {
		result.push_back(current);
		bool advanced = false;
		for (auto neighbor : quotient[current]) {
			if (has_previous && neighbor == previous) {
				continue;
			}
			previous = current;
			has_previous = true;
			current = neighbor;
			advanced = true;
			break;
		}
		if (!advanced) {
			break;
		}
	}
	// Fewer than expected means the quotient graph is several chains rather than one.
	return result.size() == expected;
}

unique_ptr<ShapeTree> Materialize(ShapeSubset subset, const vector<ShapeSubset> &split) {
	if (subset != 0 && (subset & (subset - 1)) == 0) {
		return make_uniq<ShapeTree>(CountZeros<ShapeSubset>::Trailing(subset));
	}
	const auto left = split[subset];
	return make_uniq<ShapeTree>(Materialize(left, split), Materialize(subset ^ left, split));
}

} // namespace

unique_ptr<HelixShape> DetectHelix(idx_t relation_count, const vector<ShapeEdge> &edges) {
	// A helix of m diamonds has 3m + 1 relations and 4m edges.
	if (relation_count < 4 || (relation_count - 1) % 3 != 0) {
		return nullptr;
	}
	const auto diamonds = (relation_count - 1) / 3;
	if (edges.size() != 4 * diamonds) {
		return nullptr;
	}

	const auto adjacency = BuildShapeAdjacency(relation_count, edges);
	if (adjacency.empty() || !IsShapeConnected(adjacency)) {
		return nullptr;
	}

	// A link has degree two, and its partner is the other relation with the same two neighbours.
	vector<pair<pair<idx_t, idx_t>, vector<idx_t>>> groups;
	for (idx_t relation = 0; relation < relation_count; relation++) {
		if (adjacency[relation].size() != 2) {
			continue;
		}
		const auto key = make_pair(adjacency[relation][0], adjacency[relation][1]);
		bool merged = false;
		for (auto &group : groups) {
			if (group.first == key) {
				group.second.push_back(relation);
				merged = true;
				break;
			}
		}
		if (!merged) {
			groups.emplace_back(key, vector<idx_t> {relation});
		}
	}

	vector<pair<idx_t, idx_t>> links;
	for (auto &group : groups) {
		if (group.second.size() == 2) {
			links.emplace_back(group.second[0], group.second[1]);
		}
	}

	// One diamond is a four-cycle, so both opposite pairs group this way and either could be the
	// spine. Keep the reading that leaves the lowest relation on the spine.
	if (diamonds == 1 && links.size() == 2) {
		vector<pair<idx_t, idx_t>> filtered;
		for (auto &link : links) {
			if (link.first != 0 && link.second != 0) {
				filtered.push_back(link);
			}
		}
		links = std::move(filtered);
	}
	if (links.size() != diamonds) {
		return nullptr;
	}

	// Everything not a link is spine. Without this count a link could pair with another link.
	vector<bool> is_link(relation_count, false);
	idx_t link_count = 0;
	for (auto &link : links) {
		is_link[link.first] = true;
		is_link[link.second] = true;
	}
	for (idx_t relation = 0; relation < relation_count; relation++) {
		link_count += is_link[relation] ? 1 : 0;
	}
	if (link_count != 2 * diamonds) {
		return nullptr;
	}

	// The quotient graph: one edge per diamond, joining the spine relations its links run between.
	vector<vector<idx_t>> quotient(relation_count);
	vector<pair<idx_t, idx_t>> collapsed;
	for (auto &link : links) {
		const auto &ends = adjacency[link.first];
		if (ends.size() != 2 || is_link[ends[0]] || is_link[ends[1]]) {
			return nullptr;
		}
		quotient[ends[0]].push_back(ends[1]);
		quotient[ends[1]].push_back(ends[0]);
		collapsed.emplace_back(ends[0], ends[1]);
	}

	auto result = make_uniq<HelixShape>();
	if (!SpinePath(quotient, is_link, diamonds + 1, result->spine)) {
		return nullptr;
	}

	// Order the diamonds along the chain, so links[i] runs between spine[i] and spine[i + 1].
	for (idx_t step = 0; step + 1 < result->spine.size(); step++) {
		const auto from = result->spine[step];
		const auto to = result->spine[step + 1];
		bool found = false;
		for (idx_t index = 0; index < collapsed.size(); index++) {
			const auto &ends = collapsed[index];
			if ((ends.first == from && ends.second == to) || (ends.first == to && ends.second == from)) {
				auto link = links[index];
				result->links.emplace_back(MinValue(link.first, link.second), MaxValue(link.first, link.second));
				found = true;
				break;
			}
		}
		if (!found) {
			return nullptr;
		}
	}
	return result;
}

unique_ptr<ShapePlan> SolveHelix(const ShapeGraph &graph, idx_t max_relations) {
	if (!graph.IsUsable()) {
		return nullptr;
	}
	const auto count = graph.RelationCount();
	const auto limit = MinValue(max_relations, MAX_HELIX_RELATIONS);
	if (count < 2 || count > limit) {
		return nullptr;
	}

	const ShapeSubset all = Bit(count) - 1;
	vector<double> cost(all + 1, INFINITE_COST);
	vector<bool> connected(all + 1, false);
	vector<ShapeSubset> split(all + 1, 0);

	// Ascending order suffices: every proper subset of a subset is numerically smaller than it.
	for (ShapeSubset subset = 1; subset <= all; subset++) {
		if (!graph.IsConnected(subset)) {
			continue;
		}
		connected[subset] = true;
		if ((subset & (subset - 1)) == 0) {
			cost[subset] = 0;
			continue;
		}

		// Pin the lowest relation left so each unordered split is visited once, not twice.
		const ShapeSubset anchor = subset & (~subset + 1);
		const ShapeSubset rest = subset ^ anchor;
		ShapeSubset extra = 0;
		auto best = INFINITE_COST;
		bool found = false;
		while (true) {
			const ShapeSubset left = anchor | extra;
			const ShapeSubset right = subset ^ left;
			if (right != 0 && connected[left] && connected[right]) {
				// The subset is connected and these halves partition it, so an edge must cross.
				D_ASSERT(graph.Crosses(left, right));
				const auto candidate = cost[left] + cost[right];
				// Strictly less, so the lowest left half wins ties and the plan is deterministic.
				if (!found || candidate < best) {
					found = true;
					best = candidate;
					split[subset] = left;
				}
			}
			if (extra == rest) {
				break;
			}
			// The next subset of rest, ascending.
			extra = ((extra | ~rest) + 1) & rest;
		}
		if (found) {
			cost[subset] = best + graph.SubsetSize(subset);
		}
	}

	const auto total = cost[all];
	if (!std::isfinite(total) || !connected[all]) {
		return nullptr;
	}
	auto plan = make_uniq<ShapePlan>();
	plan->tree = Materialize(all, split);
	plan->cost = total;
	return plan;
}

} // namespace duckdb

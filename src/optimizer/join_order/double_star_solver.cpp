#include "duckdb/optimizer/join_order/shape_solver.hpp"

#include "duckdb/common/algorithm.hpp"

#include <cmath>

namespace duckdb {

// Stable, so ties keep input order and two runs cannot disagree on the chosen plan.
void SortByFanout(vector<Spoke> &spokes) {
	std::stable_sort(spokes.begin(), spokes.end(),
	                 [](const Spoke &a, const Spoke &b) { return a.Fanout() < b.Fanout(); });
}

ChainResult ChainCostAndSize(double hub_weight, const vector<Spoke> &spokes) {
	auto sorted = spokes;
	SortByFanout(sorted);

	ChainResult result;
	result.size = hub_weight;
	for (auto &spoke : sorted) {
		// Two multiplications rather than one: (size * weight) * selectivity rounds differently.
		result.cost += result.size * spoke.weight * spoke.selectivity;
		result.size *= spoke.weight;
		result.size *= spoke.selectivity;
		result.order.push_back(spoke.id);
	}
	return result;
}

static vector<Spoke> Prefix(const vector<Spoke> &spokes, idx_t count) {
	vector<Spoke> result;
	for (idx_t i = 0; i < count; i++) {
		result.push_back(spokes[i]);
	}
	return result;
}

static vector<Spoke> Suffix(const vector<Spoke> &spokes, idx_t from) {
	vector<Spoke> result;
	for (idx_t i = from; i < spokes.size(); i++) {
		result.push_back(spokes[i]);
	}
	return result;
}

SideTable BuildSideTable(double hub_weight, const vector<Spoke> &sorted_spokes, bool has_extra, const Spoke &extra) {
	SideTable table;
	for (idx_t p = 0; p <= sorted_spokes.size(); p++) {
		auto before_merge = Prefix(sorted_spokes, p);
		if (has_extra) {
			// Appended rather than pinned last, so it is re-sorted into its fanout position.
			before_merge.push_back(extra);
		}
		auto chain = ChainCostAndSize(hub_weight, before_merge);
		table.cost.push_back(chain.cost);
		table.size.push_back(chain.size);
	}
	return table;
}

//! Deferred spokes of both hubs pool together, since the merge made them one cluster.
static vector<Spoke> MergedLeftovers(const vector<Spoke> &deferred_a, const vector<Spoke> &deferred_b) {
	vector<Spoke> leftovers = deferred_a;
	leftovers.insert(leftovers.end(), deferred_b.begin(), deferred_b.end());
	SortByFanout(leftovers);
	return leftovers;
}

double LeftoverMultiplier(const vector<Spoke> &deferred_a, const vector<Spoke> &deferred_b) {
	auto leftovers = MergedLeftovers(deferred_a, deferred_b);
	double multiplier = 1;
	double running = 1;
	for (auto &spoke : leftovers) {
		// Associates differently from ChainCostAndSize above, which is intentional.
		running *= spoke.weight * spoke.selectivity;
		multiplier += running;
	}
	return multiplier;
}

namespace {

//! The best pair of split points found for one assignment of the central relation.
struct SideSolution {
	double cost = 0;
	//! Left-hub spokes absorbed before the merge.
	idx_t p = 0;
	//! Right-hub spokes absorbed before the merge.
	idx_t q = 0;
};

unique_ptr<ShapeTree> Leaf(idx_t relation) {
	return make_uniq<ShapeTree>(relation);
}

unique_ptr<ShapeTree> Join(unique_ptr<ShapeTree> left, unique_ptr<ShapeTree> right) {
	return make_uniq<ShapeTree>(std::move(left), std::move(right));
}

//! One assignment of the central relation to a hub. Left absorbs it before the merge.
struct Orientation {
	idx_t left_hub;
	double left_hub_weight;
	idx_t right_hub;
	double right_hub_weight;
	idx_t central;
	double central_weight;
	//! Selectivity of the central-to-left-hub edge.
	double sel_left;
	//! Selectivity of the central-to-right-hub edge, which is the merge itself.
	double sel_right;
	vector<Spoke> left_spokes;
	vector<Spoke> right_spokes;

	//! The central relation seen as a spoke of the left hub.
	Spoke SharedEntry() const {
		return Spoke {central, central_weight, sel_left};
	}

	//! Search every split point pair. False when no candidate had a finite cost.
	bool Solve(SideSolution &result) const {
		const auto left_table = BuildSideTable(left_hub_weight, left_spokes, true, SharedEntry());
		const auto right_table = BuildSideTable(right_hub_weight, right_spokes, false, Spoke {0, 0, 0});

		bool found = false;
		for (idx_t p = 0; p <= left_spokes.size(); p++) {
			for (idx_t q = 0; q <= right_spokes.size(); q++) {
				const auto critical_size = left_table.size[p] * right_table.size[q] * sel_right;
				const auto multiplier = LeftoverMultiplier(Suffix(left_spokes, p), Suffix(right_spokes, q));
				const auto total = left_table.cost[p] + right_table.cost[q] + critical_size * multiplier;
				if (!std::isfinite(total)) {
					continue;
				}
				// Strictly less, so the lowest p then lowest q wins ties.
				if (!found || total < result.cost) {
					found = true;
					result.cost = total;
					result.p = p;
					result.q = q;
				}
			}
		}
		return found;
	}

	//! Expand split points into the join tree they describe.
	unique_ptr<ShapePlan> Materialize(const SideSolution &solution) const {
		// Rebuilt through ChainCostAndSize so the central relation lands where the costing assumed.
		auto before_merge = Prefix(left_spokes, solution.p);
		before_merge.push_back(SharedEntry());
		const auto left_chain = ChainCostAndSize(left_hub_weight, before_merge);
		const auto right_chain = ChainCostAndSize(right_hub_weight, Prefix(right_spokes, solution.q));

		auto left = Leaf(left_hub);
		for (auto relation : left_chain.order) {
			left = Join(std::move(left), Leaf(relation));
		}
		auto right = Leaf(right_hub);
		for (auto relation : right_chain.order) {
			right = Join(std::move(right), Leaf(relation));
		}

		auto top = Join(std::move(left), std::move(right));
		for (auto &spoke : MergedLeftovers(Suffix(left_spokes, solution.p), Suffix(right_spokes, solution.q))) {
			top = Join(std::move(top), Leaf(spoke.id));
		}

		auto plan = make_uniq<ShapePlan>();
		plan->tree = std::move(top);
		plan->cost = solution.cost;
		return plan;
	}
};

vector<Spoke> CollectSpokes(const ShapeGraph &graph, idx_t hub, const vector<idx_t> &spokes) {
	vector<Spoke> result;
	for (auto spoke : spokes) {
		result.push_back(Spoke {spoke, graph.Weight(spoke), graph.Selectivity(hub, spoke)});
	}
	SortByFanout(result);
	return result;
}

} // namespace

vector<DoubleStarShape> DetectDoubleStars(idx_t relation_count, const vector<ShapeEdge> &edges) {
	vector<DoubleStarShape> shapes;
	// Below four relations there is only one possible shape, so there is nothing to decide.
	if (relation_count < 4) {
		return shapes;
	}
	// A connected graph with n - 1 edges is a tree. Cycles are refused: correlated predicates
	// would be multiplied as though independent, which is a wrong answer rather than a poor one.
	if (edges.size() + 1 != relation_count) {
		return shapes;
	}

	const auto adjacency = BuildShapeAdjacency(relation_count, edges);
	if (adjacency.empty() || !IsShapeConnected(adjacency)) {
		return shapes;
	}

	for (idx_t central = 0; central < relation_count; central++) {
		if (adjacency[central].size() != 2) {
			continue;
		}
		const auto hub_a = MinValue(adjacency[central][0], adjacency[central][1]);
		const auto hub_b = MaxValue(adjacency[central][0], adjacency[central][1]);

		DoubleStarShape shape;
		shape.central = central;
		shape.hub_a = hub_a;
		shape.hub_b = hub_b;
		bool valid = true;
		for (idx_t relation = 0; relation < relation_count && valid; relation++) {
			if (relation == central || relation == hub_a || relation == hub_b) {
				continue;
			}
			// Anything hanging off a spoke, or bridging the hubs a second time, disqualifies this reading.
			if (adjacency[relation].size() != 1) {
				valid = false;
				break;
			}
			const auto neighbor = adjacency[relation][0];
			if (neighbor == hub_a) {
				shape.spokes_a.push_back(relation);
			} else if (neighbor == hub_b) {
				shape.spokes_b.push_back(relation);
			} else {
				valid = false;
			}
		}
		if (valid) {
			shapes.push_back(std::move(shape));
		}
	}
	return shapes;
}

unique_ptr<ShapePlan> SolveDoubleStar(const ShapeGraph &graph, const DoubleStarShape &shape) {
	if (!graph.IsUsable()) {
		return nullptr;
	}
	// The search is O(n * m * (n + m)) over spoke counts taken from a user query.
	if (shape.spokes_a.size() > MAX_SPOKES_PER_HUB || shape.spokes_b.size() > MAX_SPOKES_PER_HUB) {
		return nullptr;
	}

	const auto spokes_a = CollectSpokes(graph, shape.hub_a, shape.spokes_a);
	const auto spokes_b = CollectSpokes(graph, shape.hub_b, shape.spokes_b);
	const auto sel_a = graph.Selectivity(shape.hub_a, shape.central);
	const auto sel_b = graph.Selectivity(shape.hub_b, shape.central);

	// The central relation can attach to either hub, and the choice changes the cost.
	Orientation central_on_a {shape.hub_a,   graph.Weight(shape.hub_a),
	                          shape.hub_b,   graph.Weight(shape.hub_b),
	                          shape.central, graph.Weight(shape.central),
	                          sel_a,         sel_b,
	                          spokes_a,      spokes_b};
	Orientation central_on_b {shape.hub_b,   graph.Weight(shape.hub_b),
	                          shape.hub_a,   graph.Weight(shape.hub_a),
	                          shape.central, graph.Weight(shape.central),
	                          sel_b,         sel_a,
	                          spokes_b,      spokes_a};

	SideSolution solution_a;
	SideSolution solution_b;
	const auto solved_a = central_on_a.Solve(solution_a);
	const auto solved_b = central_on_b.Solve(solution_b);

	if (solved_a && (!solved_b || solution_a.cost <= solution_b.cost)) {
		return central_on_a.Materialize(solution_a);
	}
	if (solved_b) {
		return central_on_b.Materialize(solution_b);
	}
	return nullptr;
}

} // namespace duckdb

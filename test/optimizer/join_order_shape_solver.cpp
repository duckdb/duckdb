#include "catch.hpp"

#include "duckdb/optimizer/join_order/shape_solver.hpp"

#include <algorithm>
#include <cmath>

using namespace duckdb;

namespace {

// Ported from the DataFusion prototype's unit tests. Relation ids there were sparse (hubs 10 and
// 11, central 12); here a ShapeGraph is dense, so the reference cases are renumbered as
// hub_a 0, hub_b 1, central 2, spokes upward from 3, and the expected orders renumbered with them.
constexpr idx_t HUB_A = 0;
constexpr idx_t HUB_B = 1;
constexpr idx_t CENTRAL = 2;

//! Relative tolerance, sized to absorb reassociation noise but not a different search result.
constexpr double TOLERANCE = 1e-9;

bool Close(double actual, double expected) {
	if (actual == expected) {
		return true;
	}
	return std::fabs(actual - expected) <= TOLERANCE * std::fabs(expected);
}

vector<ShapeEdge> Edges(const vector<pair<idx_t, idx_t>> &pairs) {
	vector<ShapeEdge> edges;
	for (auto &entry : pairs) {
		edges.push_back(ShapeEdge {entry.first, entry.second, 0.5});
	}
	return edges;
}

//! Structure as text, so a bushy order can be asserted exactly rather than by leaf order alone.
string TreeString(const ShapeTree &tree) {
	if (tree.is_leaf) {
		return to_string(tree.relation);
	}
	return "(" + TreeString(*tree.left) + " " + TreeString(*tree.right) + ")";
}

Spoke MakeSpoke(idx_t id, double weight, double selectivity) {
	return Spoke {id, weight, selectivity};
}

//! Fanouts of 1.0, 5.0 and 0.2, spanning neutral, growing and shrinking.
vector<Spoke> ReferenceSpokes() {
	return {MakeSpoke(3, 50.0, 0.02), MakeSpoke(4, 10.0, 0.5), MakeSpoke(5, 200.0, 0.001)};
}

vector<Spoke> SortedReferenceSpokes() {
	auto spokes = ReferenceSpokes();
	SortByFanout(spokes);
	return spokes;
}

//! A double star built directly from weights and selectivities, bypassing detection.
struct StarInputs {
	double hub_a_weight;
	double hub_b_weight;
	double central_weight;
	double sel_a;
	double sel_b;
	vector<Spoke> spokes_a;
	vector<Spoke> spokes_b;
};

unique_ptr<ShapeGraph> BuildStarGraph(const StarInputs &inputs, DoubleStarShape &shape) {
	vector<double> weights(3 + inputs.spokes_a.size() + inputs.spokes_b.size(), 0);
	weights[HUB_A] = inputs.hub_a_weight;
	weights[HUB_B] = inputs.hub_b_weight;
	weights[CENTRAL] = inputs.central_weight;

	vector<ShapeEdge> edges;
	edges.push_back(ShapeEdge {HUB_A, CENTRAL, inputs.sel_a});
	edges.push_back(ShapeEdge {HUB_B, CENTRAL, inputs.sel_b});
	for (auto &spoke : inputs.spokes_a) {
		weights[spoke.id] = spoke.weight;
		edges.push_back(ShapeEdge {HUB_A, spoke.id, spoke.selectivity});
		shape.spokes_a.push_back(spoke.id);
	}
	for (auto &spoke : inputs.spokes_b) {
		weights[spoke.id] = spoke.weight;
		edges.push_back(ShapeEdge {HUB_B, spoke.id, spoke.selectivity});
		shape.spokes_b.push_back(spoke.id);
	}
	shape.central = CENTRAL;
	shape.hub_a = HUB_A;
	shape.hub_b = HUB_B;
	return make_uniq<ShapeGraph>(std::move(weights), std::move(edges));
}

unique_ptr<ShapeGraph> SingleDiamond() {
	return make_uniq<ShapeGraph>(vector<double> {50.0, 200.0, 1000.0, 5000.0},
	                             vector<ShapeEdge> {{2, 0, 0.01}, {2, 1, 0.005}, {0, 3, 0.002}, {1, 3, 0.001}});
}

//! Two diamonds sharing P1: A0 0, A1 1, B0 2, B1 3, P0 4, P1 5, P2 6.
unique_ptr<ShapeGraph> TwoDiamondHelix() {
	return make_uniq<ShapeGraph>(vector<double> {50.0, 80.0, 200.0, 300.0, 1000.0, 5000.0, 2000.0},
	                             vector<ShapeEdge> {{4, 0, 0.01},
	                                                {4, 2, 0.005},
	                                                {0, 5, 0.002},
	                                                {2, 5, 0.001},
	                                                {5, 1, 0.02},
	                                                {5, 3, 0.004},
	                                                {1, 6, 0.0015},
	                                                {3, 6, 0.003}});
}

//! A four relation path a - b - c - d, numbered in that order.
unique_ptr<ShapeGraph> PathGraph() {
	return make_uniq<ShapeGraph>(vector<double> {100.0, 1000.0, 20000.0, 300.0},
	                             vector<ShapeEdge> {{0, 1, 0.01}, {1, 2, 0.001}, {2, 3, 0.005}});
}

//! Price a tree back independently, to catch a split the search recorded wrongly.
double CostOfTree(const ShapeGraph &graph, const ShapeTree &tree, ShapeSubset &subset) {
	if (tree.is_leaf) {
		subset = ShapeSubset(1) << tree.relation;
		return 0;
	}
	ShapeSubset left_subset = 0;
	ShapeSubset right_subset = 0;
	const auto left_cost = CostOfTree(graph, *tree.left, left_subset);
	const auto right_cost = CostOfTree(graph, *tree.right, right_subset);
	subset = left_subset | right_subset;
	return left_cost + right_cost + graph.SubsetSize(subset);
}

} // namespace

//===--------------------------------------------------------------------===//
// Double star shape detection
//===--------------------------------------------------------------------===//

TEST_CASE("Double star detects the canonical bowtie", "[optimizer][join_order]") {
	//   1  2        4  5
	//    \ |        | /
	//      0 -- 3 -- 6         0 and 6 are hubs, 3 is central
	auto shapes = DetectDoubleStars(7, Edges({{0, 1}, {0, 2}, {0, 3}, {3, 6}, {6, 4}, {6, 5}}));

	REQUIRE(shapes.size() == 1);
	REQUIRE(shapes[0].central == 3);
	REQUIRE(shapes[0].hub_a == 0);
	REQUIRE(shapes[0].hub_b == 6);
	REQUIRE(shapes[0].spokes_a == vector<idx_t> {1, 2});
	REQUIRE(shapes[0].spokes_b == vector<idx_t> {4, 5});
}

TEST_CASE("Double star reads a four path two ways", "[optimizer][join_order]") {
	// 0 - 1 - 2 - 3 has two degree-two relations, so both readings are valid.
	auto shapes = DetectDoubleStars(4, Edges({{0, 1}, {1, 2}, {2, 3}}));

	REQUIRE(shapes.size() == 2);
	REQUIRE(shapes[0].central == 1);
	REQUIRE(shapes[1].central == 2);
}

TEST_CASE("Double star reads a five path one way", "[optimizer][join_order]") {
	// Only the middle relation leaves every other one hanging off a hub.
	auto shapes = DetectDoubleStars(5, Edges({{0, 1}, {1, 2}, {2, 3}, {3, 4}}));

	REQUIRE(shapes.size() == 1);
	REQUIRE(shapes[0].central == 2);
}

TEST_CASE("Double star rejects a cycle", "[optimizer][join_order]") {
	REQUIRE(DetectDoubleStars(4, Edges({{0, 1}, {1, 2}, {2, 3}, {3, 0}})).empty());
}

TEST_CASE("Double star rejects a disconnected graph", "[optimizer][join_order]") {
	// Four relations and three edges, but one relation is isolated.
	REQUIRE(DetectDoubleStars(5, Edges({{0, 1}, {1, 2}, {2, 0}, {3, 4}})).empty());
}

TEST_CASE("Double star rejects a single star", "[optimizer][join_order]") {
	// One hub with three spokes has no bridging relation.
	REQUIRE(DetectDoubleStars(4, Edges({{0, 1}, {0, 2}, {0, 3}})).empty());
}

TEST_CASE("Double star rejects a triple star", "[optimizer][join_order]") {
	//   1     3     5        three hubs chained by two centrals
	//   |     |     |
	//   0 - 2 - 4 - 6        hub 4 sits between two bridges
	REQUIRE(DetectDoubleStars(8, Edges({{0, 1}, {0, 2}, {2, 4}, {4, 3}, {4, 6}, {6, 5}, {6, 7}})).empty());
}

TEST_CASE("Double star rejects a spoke with its own child", "[optimizer][join_order]") {
	// The canonical bowtie, but spoke 1 carries a child of its own:
	//
	//   7 - 1  2            5  6
	//        \ |            | /
	//          0 ---- 3 ---- 4
	//
	// Relation 1 now has degree two, so neither candidate works: central 1 is stranded by
	// relation 3, and central 3 by relation 1. This needs the full bowtie, since a smaller graph
	// tends to admit some other valid assignment of roles.
	REQUIRE(DetectDoubleStars(8, Edges({{0, 1}, {0, 2}, {0, 3}, {3, 4}, {4, 5}, {4, 6}, {1, 7}})).empty());
}

TEST_CASE("Double star rejects graphs too small to reorder", "[optimizer][join_order]") {
	REQUIRE(DetectDoubleStars(3, Edges({{0, 1}, {1, 2}})).empty());
	REQUIRE(DetectDoubleStars(2, Edges({{0, 1}})).empty());
}

//===--------------------------------------------------------------------===//
// Double star cost model
//===--------------------------------------------------------------------===//

TEST_CASE("Chain joins cheapest fanout first", "[optimizer][join_order]") {
	// By hand: 1000 -> (x0.2) 200 -> (x1.0) 200 -> (x5.0) 1000, paying 200 + 200 + 1000.
	auto chain = ChainCostAndSize(1000.0, ReferenceSpokes());

	REQUIRE(chain.cost == 1400.0);
	REQUIRE(chain.size == 1000.0);
	REQUIRE(chain.order == vector<idx_t> {5, 3, 4});
}

TEST_CASE("Chain of nothing is free", "[optimizer][join_order]") {
	auto chain = ChainCostAndSize(1000.0, vector<Spoke> {});

	REQUIRE(chain.cost == 0.0);
	REQUIRE(chain.size == 1000.0);
	REQUIRE(chain.order.empty());
}

TEST_CASE("Chain order beats the worst order", "[optimizer][join_order]") {
	auto sorted = SortedReferenceSpokes();
	std::reverse(sorted.begin(), sorted.end());
	double reversed = 0;
	double size = 1000.0;
	for (auto &spoke : sorted) {
		reversed += size * spoke.weight * spoke.selectivity;
		size *= spoke.weight;
		size *= spoke.selectivity;
	}

	REQUIRE(ChainCostAndSize(1000.0, ReferenceSpokes()).cost == 1400.0);
	REQUIRE(reversed == 11000.0);
}

TEST_CASE("Side table prices every split point", "[optimizer][join_order]") {
	auto table = BuildSideTable(1000.0, SortedReferenceSpokes(), false, MakeSpoke(0, 0, 0));

	REQUIRE(table.cost == vector<double> {0.0, 200.0, 400.0, 1400.0});
	REQUIRE(table.size == vector<double> {1000.0, 200.0, 200.0, 1000.0});
}

TEST_CASE("Side table places the central relation by fanout", "[optimizer][join_order]") {
	// Fanout 3.0 sorts after the 0.2 and 1.0 spokes but before the 5.0 one.
	auto table = BuildSideTable(1000.0, SortedReferenceSpokes(), true, MakeSpoke(CENTRAL, 300.0, 0.01));

	REQUIRE(table.cost == vector<double> {3000.0, 800.0, 1000.0, 4000.0});
	REQUIRE(table.size == vector<double> {3000.0, 600.0, 600.0, 3000.0});
}

TEST_CASE("Leftover multiplier pools both piles", "[optimizer][join_order]") {
	vector<Spoke> deferred_a {MakeSpoke(3, 50.0, 0.02)};                         // fanout 1.0
	vector<Spoke> deferred_b {MakeSpoke(6, 10.0, 0.5), MakeSpoke(7, 4.0, 0.25)}; // 5.0 and 1.0

	// Merged order is 1.0, 1.0, 5.0, so the multiplier is 1 + 1 + 1 + 5.
	REQUIRE(LeftoverMultiplier(deferred_a, deferred_b) == 8.0);
}

TEST_CASE("Leftover multiplier of nothing is the merge alone", "[optimizer][join_order]") {
	REQUIRE(LeftoverMultiplier(vector<Spoke> {}, vector<Spoke> {}) == 1.0);
}

TEST_CASE("Double star solves the symmetric reference case", "[optimizer][join_order]") {
	// Every spoke is worth absorbing before the merge, so both split points sit at their maximum.
	StarInputs inputs {
	    1000.0, 2000.0, 300.0, 0.01, 0.005, ReferenceSpokes(), {MakeSpoke(6, 20.0, 0.1), MakeSpoke(7, 5.0, 0.4)}};
	DoubleStarShape shape;
	auto graph = BuildStarGraph(inputs, shape);
	auto plan = SolveDoubleStar(*graph, shape);

	REQUIRE(plan);
	REQUIRE(plan->cost == 136000.0);
	// hub_a then its three spokes and the central relation in fanout order 0.2, 1.0, 3.0, 5.0,
	// then hub_b with its two spokes, and nothing deferred.
	REQUIRE(plan->tree->Leaves() == vector<idx_t> {HUB_A, 5, 3, CENTRAL, 4, HUB_B, 6, 7});
}

TEST_CASE("Double star picks the cheaper orientation", "[optimizer][join_order]") {
	// Attaching the central relation to the other hub is worth 35% here.
	StarInputs inputs {500000.0, 10.0, 100.0, 0.5, 0.001, {MakeSpoke(3, 1000.0, 0.9)}, {MakeSpoke(4, 2.0, 0.1)}};
	DoubleStarShape shape;
	auto graph = BuildStarGraph(inputs, shape);
	auto plan = SolveDoubleStar(*graph, shape);

	REQUIRE(plan);
	REQUIRE(Close(plan->cost, 45050001.2));
	// hub_b leads, taking the central relation and its own spoke; the fanout-900 spoke is deferred.
	REQUIRE(plan->tree->Leaves() == vector<idx_t> {HUB_B, CENTRAL, 4, HUB_A, 3});
}

TEST_CASE("Double star defers growing spokes past the merge", "[optimizer][join_order]") {
	StarInputs inputs {1000.0, 1000.0, 100.0, 0.01, 0.01, {MakeSpoke(3, 10.0, 0.001), MakeSpoke(4, 1000.0, 5.0)}, {}};
	DoubleStarShape shape;
	auto graph = BuildStarGraph(inputs, shape);
	auto plan = SolveDoubleStar(*graph, shape);

	REQUIRE(plan);
	// The shrinking spoke is absorbed before the merge, the fanout-5000 one after.
	auto leaves = plan->tree->Leaves();
	REQUIRE(leaves.back() == 4);
	REQUIRE(std::find(leaves.begin(), leaves.end(), idx_t(3)) < std::find(leaves.begin(), leaves.end(), HUB_B));
}

TEST_CASE("Double star plan visits every relation exactly once", "[optimizer][join_order]") {
	StarInputs inputs {
	    1000.0, 2000.0, 300.0, 0.01, 0.005, ReferenceSpokes(), {MakeSpoke(6, 20.0, 0.1), MakeSpoke(7, 5.0, 0.4)}};
	DoubleStarShape shape;
	auto graph = BuildStarGraph(inputs, shape);
	auto plan = SolveDoubleStar(*graph, shape);

	REQUIRE(plan);
	auto leaves = plan->tree->Leaves();
	std::sort(leaves.begin(), leaves.end());
	REQUIRE(leaves == vector<idx_t> {0, 1, 2, 3, 4, 5, 6, 7});
}

TEST_CASE("Double star with no spokes is just the merge", "[optimizer][join_order]") {
	StarInputs inputs {100.0, 200.0, 50.0, 0.1, 0.2, {}, {}};
	DoubleStarShape shape;
	auto graph = BuildStarGraph(inputs, shape);
	auto plan = SolveDoubleStar(*graph, shape);

	REQUIRE(plan);
	REQUIRE(plan->tree->Leaves() == vector<idx_t> {HUB_A, CENTRAL, HUB_B});
}

TEST_CASE("Double star rejects unusable statistics", "[optimizer][join_order]") {
	// A zero distinct count surfaces as an infinite selectivity, absent statistics as NaN.
	DoubleStarShape shape;
	shape.central = CENTRAL;
	shape.hub_a = HUB_A;
	shape.hub_b = HUB_B;
	ShapeGraph infinite(
	    vector<double> {1000.0, 2000.0, 300.0},
	    vector<ShapeEdge> {{HUB_A, CENTRAL, std::numeric_limits<double>::infinity()}, {HUB_B, CENTRAL, 0.005}});
	REQUIRE(!SolveDoubleStar(infinite, shape));

	ShapeGraph negative(vector<double> {1000.0, -1.0, 300.0},
	                    vector<ShapeEdge> {{HUB_A, CENTRAL, 0.01}, {HUB_B, CENTRAL, 0.005}});
	REQUIRE(!SolveDoubleStar(negative, shape));
}

TEST_CASE("Double star with empty relations costs nothing", "[optimizer][join_order]") {
	StarInputs inputs {0.0, 0.0, 0.0, 0.5, 0.5, {}, {}};
	DoubleStarShape shape;
	auto graph = BuildStarGraph(inputs, shape);
	auto plan = SolveDoubleStar(*graph, shape);

	REQUIRE(plan);
	REQUIRE(plan->cost == 0.0);
}

TEST_CASE("Double star resolves tied costs deterministically", "[optimizer][join_order]") {
	// Every spoke has the same fanout, so only the tie break decides the order.
	StarInputs inputs {1000.0,
	                   1000.0,
	                   100.0,
	                   0.01,
	                   0.01,
	                   {MakeSpoke(3, 10.0, 0.1), MakeSpoke(4, 10.0, 0.1)},
	                   {MakeSpoke(5, 10.0, 0.1), MakeSpoke(6, 10.0, 0.1)}};
	DoubleStarShape first_shape;
	auto first_graph = BuildStarGraph(inputs, first_shape);
	auto first = SolveDoubleStar(*first_graph, first_shape);
	DoubleStarShape second_shape;
	auto second_graph = BuildStarGraph(inputs, second_shape);
	auto second = SolveDoubleStar(*second_graph, second_shape);

	REQUIRE(first);
	REQUIRE(second);
	REQUIRE(TreeString(*first->tree) == TreeString(*second->tree));
	REQUIRE(first->cost == second->cost);
}

TEST_CASE("Double star cheapest fanout first beats every other order", "[optimizer][join_order]") {
	auto spokes = ReferenceSpokes();
	const auto best = ChainCostAndSize(1000.0, spokes).cost;

	auto permuted = spokes;
	std::sort(permuted.begin(), permuted.end(), [](const Spoke &a, const Spoke &b) { return a.id < b.id; });
	do {
		double cost = 0;
		double size = 1000.0;
		for (auto &spoke : permuted) {
			cost += size * spoke.weight * spoke.selectivity;
			size *= spoke.weight;
			size *= spoke.selectivity;
		}
		REQUIRE(best <= cost);
	} while (std::next_permutation(permuted.begin(), permuted.end(),
	                               [](const Spoke &a, const Spoke &b) { return a.id < b.id; }));
}

//===--------------------------------------------------------------------===//
// Helix shape detection
//===--------------------------------------------------------------------===//

TEST_CASE("Helix detects a two diamond helix", "[optimizer][join_order]") {
	// Spine 0 - 3 - 6, links {1,2} and {4,5}.
	auto shape = DetectHelix(7, Edges({{0, 1}, {0, 2}, {1, 3}, {2, 3}, {3, 4}, {3, 5}, {4, 6}, {5, 6}}));

	REQUIRE(shape);
	REQUIRE(shape->spine == vector<idx_t> {0, 3, 6});
	REQUIRE(shape->links.size() == 2);
	REQUIRE(shape->links[0] == make_pair(idx_t(1), idx_t(2)));
	REQUIRE(shape->links[1] == make_pair(idx_t(4), idx_t(5)));
}

TEST_CASE("Helix detects a three diamond helix", "[optimizer][join_order]") {
	auto shape = DetectHelix(
	    10, Edges({{0, 1}, {0, 2}, {1, 3}, {2, 3}, {3, 4}, {3, 5}, {4, 6}, {5, 6}, {6, 7}, {6, 8}, {7, 9}, {8, 9}}));

	REQUIRE(shape);
	REQUIRE(shape->spine == vector<idx_t> {0, 3, 6, 9});
	REQUIRE(shape->links.size() == 3);
}

TEST_CASE("Helix accepts a single diamond", "[optimizer][join_order]") {
	// A four cycle: both opposite pairs group as links, and the reading keeping relation 0 on the
	// spine is the one taken.
	auto shape = DetectHelix(4, Edges({{0, 1}, {0, 2}, {1, 3}, {2, 3}}));

	REQUIRE(shape);
	REQUIRE(shape->spine == vector<idx_t> {0, 3});
	REQUIRE(shape->links.size() == 1);
	REQUIRE(shape->links[0] == make_pair(idx_t(1), idx_t(2)));
}

TEST_CASE("Helix reports the spine in chain order not index order", "[optimizer][join_order]") {
	// Spine relations 6, 0, 3 in chain order, which is not their index order.
	auto shape = DetectHelix(7, Edges({{6, 1}, {6, 2}, {1, 0}, {2, 0}, {0, 4}, {0, 5}, {4, 3}, {5, 3}}));

	REQUIRE(shape);
	REQUIRE(shape->spine == vector<idx_t> {3, 0, 6});
}

TEST_CASE("Helix rejects a relation count that is not three m plus one", "[optimizer][join_order]") {
	REQUIRE(!DetectHelix(6, Edges({{0, 1}, {0, 2}, {1, 3}, {2, 3}, {3, 4}, {4, 5}})));
}

TEST_CASE("Helix rejects the right relation count with the wrong edge count", "[optimizer][join_order]") {
	// Seven relations wants eight edges, not six.
	REQUIRE(!DetectHelix(7, Edges({{0, 1}, {1, 2}, {2, 3}, {3, 4}, {4, 5}, {5, 6}})));
}

TEST_CASE("Helix rejects a bowtie", "[optimizer][join_order]") {
	REQUIRE(!DetectHelix(7, Edges({{0, 1}, {0, 2}, {0, 3}, {3, 6}, {6, 4}, {6, 5}})));
}

TEST_CASE("Helix rejects a disconnected graph", "[optimizer][join_order]") {
	// Two independent diamonds: the right counts, but no single chain.
	REQUIRE(!DetectHelix(7, Edges({{0, 1}, {0, 2}, {1, 3}, {2, 3}, {4, 5}, {4, 6}, {5, 6}, {5, 6}})));
}

TEST_CASE("Helix rejects a ring of diamonds", "[optimizer][join_order]") {
	// Three diamonds joined head to tail: the spine is a cycle, not a path.
	REQUIRE(!DetectHelix(
	    10, Edges({{0, 1}, {0, 2}, {1, 3}, {2, 3}, {3, 4}, {3, 5}, {4, 6}, {5, 6}, {6, 7}, {6, 8}, {7, 0}, {8, 0}})));
}

TEST_CASE("Helix rejects a diamond whose links touch each other", "[optimizer][join_order]") {
	REQUIRE(!DetectHelix(4, Edges({{0, 1}, {1, 2}, {2, 3}, {1, 2}})));
}

TEST_CASE("Helix rejects graphs too small to be a helix", "[optimizer][join_order]") {
	REQUIRE(!DetectHelix(3, Edges({{0, 1}, {1, 2}})));
	REQUIRE(!DetectHelix(2, Edges({{0, 1}})));
}

//===--------------------------------------------------------------------===//
// Helix cost model
//===--------------------------------------------------------------------===//

TEST_CASE("Helix matches the reference on a single diamond", "[optimizer][join_order]") {
	auto graph = SingleDiamond();
	auto plan = SolveHelix(*graph, MAX_HELIX_RELATIONS);

	REQUIRE(plan);
	REQUIRE(Close(plan->cost, 605.0));
	// (((A0 P1) B0) P0): the two smallest first, then the diamond closes before P0 is absorbed.
	REQUIRE(TreeString(*plan->tree) == "(((0 3) 1) 2)");
}

TEST_CASE("Helix matches the reference on a two diamond helix", "[optimizer][join_order]") {
	auto graph = TwoDiamondHelix();
	auto plan = SolveHelix(*graph, MAX_HELIX_RELATIONS);

	REQUIRE(plan);
	// The reference prints 552.85439999999994; the digits past this are reassociation noise.
	REQUIRE(Close(plan->cost, 552.8544));
	REQUIRE(TreeString(*plan->tree) == "(((0 (((1 6) 3) 5)) 2) 4)");
}

TEST_CASE("Helix matches the reference on a path", "[optimizer][join_order]") {
	auto graph = PathGraph();
	auto plan = SolveHelix(*graph, MAX_HELIX_RELATIONS);

	REQUIRE(plan);
	REQUIRE(Close(plan->cost, 51000.0));
	REQUIRE(TreeString(*plan->tree) == "(((0 1) 2) 3)");
}

TEST_CASE("Helix gives two relations a single order", "[optimizer][join_order]") {
	ShapeGraph graph(vector<double> {100.0, 1000.0}, vector<ShapeEdge> {{0, 1, 0.01}});
	auto plan = SolveHelix(graph, MAX_HELIX_RELATIONS);

	REQUIRE(plan);
	REQUIRE(Close(plan->cost, 1000.0));
	REQUIRE(TreeString(*plan->tree) == "(0 1)");
}

TEST_CASE("Helix chosen tree costs what the search said", "[optimizer][join_order]") {
	// The recorded splits are the one piece with nothing to compare against, so price them back.
	for (auto &graph : {SingleDiamond(), TwoDiamondHelix(), PathGraph()}) {
		auto plan = SolveHelix(*graph, MAX_HELIX_RELATIONS);
		REQUIRE(plan);
		ShapeSubset covered = 0;
		REQUIRE(Close(CostOfTree(*graph, *plan->tree, covered), plan->cost));
	}
}

TEST_CASE("Helix covers every relation exactly once", "[optimizer][join_order]") {
	for (auto &graph : {SingleDiamond(), TwoDiamondHelix(), PathGraph()}) {
		auto plan = SolveHelix(*graph, MAX_HELIX_RELATIONS);
		REQUIRE(plan);
		auto leaves = plan->tree->Leaves();
		std::sort(leaves.begin(), leaves.end());
		REQUIRE(leaves.size() == graph->RelationCount());
		for (idx_t relation = 0; relation < graph->RelationCount(); relation++) {
			REQUIRE(leaves[relation] == relation);
		}
	}
}

TEST_CASE("Helix gives every join a predicate", "[optimizer][join_order]") {
	// Both halves of every split are connected and an edge crosses between them, so the emitted
	// tree contains no cross product.
	for (auto &graph : {SingleDiamond(), TwoDiamondHelix(), PathGraph()}) {
		auto plan = SolveHelix(*graph, MAX_HELIX_RELATIONS);
		REQUIRE(plan);
		ShapeSubset covered = 0;
		CostOfTree(*graph, *plan->tree, covered);
		REQUIRE(graph->IsConnected(covered));
	}
}

TEST_CASE("Helix finds a bushy order", "[optimizer][join_order]") {
	// A left deep search cannot produce this tree; the two diamond helix joins two composites.
	auto graph = TwoDiamondHelix();
	auto plan = SolveHelix(*graph, MAX_HELIX_RELATIONS);

	REQUIRE(plan);
	REQUIRE(!plan->tree->left->is_leaf);
	REQUIRE(!plan->tree->left->left->is_leaf);
	REQUIRE(!plan->tree->left->left->right->is_leaf);
}

TEST_CASE("Helix is deterministic", "[optimizer][join_order]") {
	auto first_graph = TwoDiamondHelix();
	auto second_graph = TwoDiamondHelix();
	auto first = SolveHelix(*first_graph, MAX_HELIX_RELATIONS);
	auto second = SolveHelix(*second_graph, MAX_HELIX_RELATIONS);

	REQUIRE(first);
	REQUIRE(second);
	REQUIRE(TreeString(*first->tree) == TreeString(*second->tree));
	REQUIRE(first->cost == second->cost);
}

TEST_CASE("Helix declines a disconnected graph", "[optimizer][join_order]") {
	ShapeGraph graph(vector<double> {100.0, 200.0, 300.0}, vector<ShapeEdge> {{0, 1, 0.01}});
	REQUIRE(!SolveHelix(graph, MAX_HELIX_RELATIONS));
}

TEST_CASE("Helix declines more relations than the cap", "[optimizer][join_order]") {
	auto graph = TwoDiamondHelix();
	REQUIRE(!SolveHelix(*graph, 6));
	REQUIRE(SolveHelix(*graph, 7));
}

TEST_CASE("Helix declines fewer than two relations", "[optimizer][join_order]") {
	ShapeGraph graph(vector<double> {100.0}, vector<ShapeEdge> {});
	REQUIRE(!SolveHelix(graph, MAX_HELIX_RELATIONS));
}

TEST_CASE("Helix declines unusable numbers", "[optimizer][join_order]") {
	ShapeGraph nan_weight(vector<double> {100.0, std::nan("")}, vector<ShapeEdge> {{0, 1, 0.01}});
	REQUIRE(!SolveHelix(nan_weight, MAX_HELIX_RELATIONS));

	ShapeGraph infinite(vector<double> {100.0, 200.0},
	                    vector<ShapeEdge> {{0, 1, std::numeric_limits<double>::infinity()}});
	REQUIRE(!SolveHelix(infinite, MAX_HELIX_RELATIONS));
}

TEST_CASE("Helix declines a malformed edge", "[optimizer][join_order]") {
	// An edge naming one relation twice, and a relation pair appearing twice.
	ShapeGraph self_edge(vector<double> {100.0, 200.0}, vector<ShapeEdge> {{0, 0, 0.01}});
	REQUIRE(!SolveHelix(self_edge, MAX_HELIX_RELATIONS));

	ShapeGraph repeated(vector<double> {100.0, 200.0}, vector<ShapeEdge> {{0, 1, 0.01}, {1, 0, 0.02}});
	REQUIRE(!SolveHelix(repeated, MAX_HELIX_RELATIONS));

	ShapeGraph out_of_range(vector<double> {100.0, 200.0}, vector<ShapeEdge> {{0, 5, 0.01}});
	REQUIRE(!SolveHelix(out_of_range, MAX_HELIX_RELATIONS));
}

TEST_CASE("Helix declines when every order overflows", "[optimizer][join_order]") {
	// Cardinalities large enough that any product is infinite.
	const auto huge = std::numeric_limits<double>::max();
	ShapeGraph graph(vector<double> {huge, huge, huge}, vector<ShapeEdge> {{0, 1, 1.0}, {1, 2, 1.0}});
	auto plan = SolveHelix(graph, MAX_HELIX_RELATIONS);
	REQUIRE(!plan);
}

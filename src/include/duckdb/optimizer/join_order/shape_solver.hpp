//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/join_order/shape_solver.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

//! Ceiling on relations in a shape graph, set by the width of the neighbour bitmask.
constexpr idx_t MAX_SHAPE_RELATIONS = 64;

//! Hard ceiling on relations in a helix search, which costs 3^n time and 2^n memory.
constexpr idx_t MAX_HELIX_RELATIONS = 22;

//! Upper bound on spokes joined to one hub of a double star.
constexpr idx_t MAX_SPOKES_PER_HUB = 64;

//! A subset of relations, one bit per relation index.
using ShapeSubset = uint64_t;

//! An equijoin edge and the selectivity of the predicate connecting its two relations.
struct ShapeEdge {
	idx_t left;
	idx_t right;
	double selectivity;
};

//! A join order, as a binary tree over relation indices.
struct ShapeTree {
	explicit ShapeTree(idx_t relation);
	ShapeTree(unique_ptr<ShapeTree> left_p, unique_ptr<ShapeTree> right_p);

	//! The relations this tree covers, left to right.
	vector<idx_t> Leaves() const;

	bool is_leaf;
	idx_t relation;
	unique_ptr<ShapeTree> left;
	unique_ptr<ShapeTree> right;
};

//! A chosen join order and its estimated cost, in rows flowing through joins.
struct ShapePlan {
	unique_ptr<ShapeTree> tree;
	double cost;
};

//! A join graph priced for shape solving: a cardinality per relation, a selectivity per pair.
class ShapeGraph {
public:
	ShapeGraph(vector<double> weights_p, vector<ShapeEdge> edges_p);

public:
	idx_t RelationCount() const {
		return weights.size();
	}
	const vector<ShapeEdge> &Edges() const {
		return edges;
	}
	double Weight(idx_t relation) const {
		return weights[relation];
	}
	//! False when a weight or selectivity is absent, negative or infinite, or a pair is repeated.
	bool IsUsable() const {
		return usable;
	}
	//! Selectivity of the edge joining two relations, or 1 when none does.
	double Selectivity(idx_t left, idx_t right) const;
	//! Product of the cardinalities in the subset and of every selectivity inside it.
	double SubsetSize(ShapeSubset subset) const;
	//! Whether every relation in the subset reaches the others without leaving it.
	bool IsConnected(ShapeSubset subset) const;
	//! Whether any edge joins a relation in left to one in right.
	bool Crosses(ShapeSubset left, ShapeSubset right) const;

private:
	vector<double> weights;
	vector<ShapeEdge> edges;
	//! selectivity[i][j] for i < j, 1 where no edge joins the two.
	vector<vector<double>> selectivity;
	vector<ShapeSubset> neighbors;
	bool usable;
};

//! A relation joined to exactly one hub, with the selectivity of the edge reaching it.
struct Spoke {
	idx_t id;
	double weight;
	double selectivity;

	//! The factor by which absorbing this spoke multiplies the running size. Below 1 shrinks.
	double Fanout() const {
		return weight * selectivity;
	}
};

//! Cost, resulting size, and the order a chain of spokes was joined in.
struct ChainResult {
	double cost = 0;
	double size = 0;
	vector<idx_t> order;
};

//! Cost and size of one side of a double star for every split point.
struct SideTable {
	vector<double> cost;
	vector<double> size;
};

//! Sort spokes cheapest fanout first, stably, so ties cannot reorder between runs.
void SortByFanout(vector<Spoke> &spokes);
//! Join spokes onto a hub cheapest fanout first, which is optimal for a single star.
ChainResult ChainCostAndSize(double hub_weight, const vector<Spoke> &spokes);
//! Entry p covers joining the p cheapest spokes onto the hub, plus extra when supplied.
SideTable BuildSideTable(double hub_weight, const vector<Spoke> &sorted_spokes, bool has_extra, const Spoke &extra);
//! Multiplier pricing the merge plus every deferred spoke. The leading 1 is the merge itself.
double LeftoverMultiplier(const vector<Spoke> &deferred_a, const vector<Spoke> &deferred_b);

//! Two hubs with their single-edge spokes, bridged by a central relation.
struct DoubleStarShape {
	idx_t central;
	idx_t hub_a;
	idx_t hub_b;
	vector<idx_t> spokes_a;
	vector<idx_t> spokes_b;
};

//! A chain of diamonds: spine relations joined by pairs of parallel relations.
struct HelixShape {
	vector<idx_t> spine;
	//! links[i] runs between spine[i] and spine[i + 1].
	vector<pair<idx_t, idx_t>> links;
};

//! Neighbour lists for each relation, each sorted ascending.
vector<vector<idx_t>> BuildShapeAdjacency(idx_t relation_count, const vector<ShapeEdge> &edges);
//! Whether every relation is reachable from relation zero.
bool IsShapeConnected(const vector<vector<idx_t>> &adjacency);

//! Every valid double star decomposition of the graph, ordered by central relation.
vector<DoubleStarShape> DetectDoubleStars(idx_t relation_count, const vector<ShapeEdge> &edges);
//! The cheapest order for one decomposition, or nullptr when no candidate had a finite cost.
unique_ptr<ShapePlan> SolveDoubleStar(const ShapeGraph &graph, const DoubleStarShape &shape);

//! The graph read as a chain of diamonds, or nullptr when it is not one.
unique_ptr<HelixShape> DetectHelix(idx_t relation_count, const vector<ShapeEdge> &edges);
//! The cheapest order over all connected subgraphs, or nullptr when none had a finite cost.
unique_ptr<ShapePlan> SolveHelix(const ShapeGraph &graph, idx_t max_relations);

} // namespace duckdb

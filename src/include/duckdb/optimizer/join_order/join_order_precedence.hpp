//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/join_order/join_order_precedence.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/join_order/join_order_plan.hpp"
#include "duckdb/optimizer/join_order/join_order_rank.hpp"

namespace duckdb {

//! Generalized ranking over a witnessed scaffold; candidate legality uses the complete query graph.
class JoinOrderPrecedence {
public:
	JoinOrderPrecedence(QueryGraphManager &graph, CardinalityEstimator &estimator,
	                    JoinOrderCandidateEvaluator &evaluator, const CapturedJoinOrderPlan &witness);
	//! Build the scaffold and choose one rooted order with a legal prescribed construction.
	//! Returns an empty order when the scaffold is not separable or no rooted construction succeeds.
	vector<RelationIndex> GetBestOrder();

private:
	using Mask = vector<bool>;

	struct Edge {
		reference<JoinRelationSet> left;
		reference<JoinRelationSet> right;
		double log_selectivity;
	};

	struct Atom {
		Atom(vector<RelationIndex> relations, double multiplier, double preparation,
		     idx_t group = DConstants::INVALID_INDEX);

		vector<RelationIndex> relations;
		JoinOrderRank rank;
		double log_preparation;
		idx_t group;
	};

	struct Block {
		Block(Atom atom, vector<RelationIndex> prerequisites);

		vector<Atom> atoms;
		JoinOrderRank rank;
		vector<RelationIndex> prerequisites;
	};

	struct Node {
		explicit Node(Block block);

		Block block;
		vector<idx_t> children;
	};

	struct Group {
		vector<RelationIndex> relations;
		vector<Atom> atoms;
		double log_cardinality = 0;
		double log_preparation = 0;
	};

	struct CachedGroup {
		Mask component;
		Mask group;
		Group result;
	};

private:
	bool Build();
	RankedJoinOrder GetOrder(RelationIndex root);
	static bool EndpointLess(const JoinRelationSet &left, const JoinRelationSet &right,
	                         const JoinRelationSet &other_left, const JoinRelationSet &other_right);
	static bool Contains(const Mask &mask, const JoinRelationSet &set);
	static idx_t Count(const Mask &mask);
	static idx_t FindRoot(vector<idx_t> &parents, idx_t node);
	void Components(const Mask &component, idx_t omitted, vector<idx_t> &labels);
	bool CheckComponent(const Mask &component);
	static void Mark(const Block &block, Mask &mask);
	static double TotalWork(const vector<Block> &chain);
	idx_t SolveGroup(const JoinRelationSet &group, const Mask &component);
	Group BuildGroup(vector<Block> &chain, const Mask &group_mask);
	idx_t SolveForwardGroup(idx_t edge_idx, const JoinRelationSet &head, const Mask &component, const Mask &marked);
	bool Visit(idx_t node_idx, const Mask &component, Mask &marked, Mask path, vector<Node> &nodes);
	void Normalize(vector<Block> &chain);
	vector<Block> Solve(RelationIndex root, const Mask &component);
	vector<Block> MergeChildren(Node &node, vector<vector<Block>> &chains);
	optional_ptr<JoinRelationSet> Materialize(const vector<Atom> &atoms, JoinOrderPlanMap &plans);

private:
	QueryGraphManager &graph;
	CardinalityEstimator &estimator;
	JoinOrderCandidateEvaluator &evaluator;
	const CapturedJoinOrderPlan &witness;
	vector<double> cardinalities;
	vector<Edge> edges;
	vector<Mask> checked_components;
	vector<CachedGroup> groups;
};

} // namespace duckdb

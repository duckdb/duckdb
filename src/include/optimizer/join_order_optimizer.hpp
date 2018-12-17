//===----------------------------------------------------------------------===//
//                         DuckDB
//
// optimizer/join_order_optimizer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/join_order/query_graph.hpp"
#include "optimizer/join_order/relation.hpp"
#include "planner/logical_operator.hpp"
#include "planner/logical_operator_visitor.hpp"

#include <functional>
#include <unordered_set>

namespace duckdb {

class JoinOrderOptimizer : public LogicalOperatorVisitor {
public:
	//! Represents a node in the join plan
	struct JoinNode {
		RelationSet *set;
		NeighborInfo *info;
		size_t cardinality;
		size_t cost;
		JoinNode *left;
		JoinNode *right;

		string ToString() {
			string result = "";
			if (left) {
				result = StringUtil::Format("[%s JOIN %s] [Estimated Cardinality: %zu]\n",
				                            left->set->ToString().c_str(), right->set->ToString().c_str(), cardinality);
				result += left->ToString();
				result += right->ToString();
			}
			return result;
		}

		//! Create a leaf node in the join tree
		JoinNode(RelationSet *set, size_t cardinality)
		    : set(set), info(nullptr), cardinality(cardinality), cost(cardinality), left(nullptr), right(nullptr) {
		}
		//! Create an intermediate node in the join tree
		JoinNode(RelationSet *set, NeighborInfo *info, JoinNode *left, JoinNode *right, size_t cardinality, size_t cost)
		    : set(set), info(info), cardinality(cardinality), cost(cost), left(left), right(right) {
		}
	};

public:
	//! Perform join reordering inside a plan
	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> plan);

	using LogicalOperatorVisitor::Visit;
	unique_ptr<Expression> Visit(SubqueryExpression &expr) override;

private:
	//! The total amount of pairs currently considered
	size_t pairs = 0;
	//! Set of all relations considered in the join optimizer
	vector<unique_ptr<Relation>> relations;
	//! A mapping of base table index -> index into relations array (relation number)
	unordered_map<size_t, size_t> relation_mapping;
	//! A structure holding all the created RelationSet objects
	RelationSetManager set_manager;
	//! The set of edges used in the join optimizer
	QueryGraph query_graph;
	//! The optimal join plan found for the specific RelationSet*
	unordered_map<RelationSet *, unique_ptr<JoinNode>> plans;
	//! The set of filters extracted from the query graph
	vector<unique_ptr<Expression>> filters;
	//! The set of filter infos created from the extracted filters
	vector<unique_ptr<FilterInfo>> filter_infos;

	//! Extract the bindings referred to by an Expression
	bool ExtractBindings(Expression &expression, std::unordered_set<size_t> &bindings);
	//! Traverse the query tree to find (1) base relations, (2) existing join conditions and (3) filters that can be
	//! rewritten into joins. Returns true if there are joins in the tree that can be reordered, false otherwise.
	bool ExtractJoinRelations(LogicalOperator &input_op, vector<LogicalOperator *> &filter_operators,
	                          LogicalOperator *parent = nullptr);
	//! Emit a pair as a potential join candidate. Returns the best plan found for the (left, right) connection (either
	//! the newly created plan, or an existing plan)
	JoinNode *EmitPair(RelationSet *left, RelationSet *right, NeighborInfo *info);
	//! Tries to emit a potential join candidate pair. Returns false if too many pairs have already been emitted,
	//! cancelling the dynamic programming step.
	bool TryEmitPair(RelationSet *left, RelationSet *right, NeighborInfo *info);

	bool EnumerateCmpRecursive(RelationSet *left, RelationSet *right, std::unordered_set<size_t> exclusion_set);
	//! Emit a relation set node
	bool EmitCSG(RelationSet *node);
	//! Enumerate the possible connected subgraphs that can be joined together in the join graph
	bool EnumerateCSGRecursive(RelationSet *node, std::unordered_set<size_t> &exclusion_set);
	//! Rewrite a logical query plan given the join plan
	unique_ptr<LogicalOperator> RewritePlan(unique_ptr<LogicalOperator> plan, JoinNode *node);
	//! Generate cross product edges inside the side
	void GenerateCrossProducts();
	//! Perform the join order solving
	void SolveJoinOrder();
	//! Solve the join order exactly using dynamic programming. Returns true if it was completed successfully (i.e. did
	//! not time-out)
	bool SolveJoinOrderExactly();
	//! Solve the join order approximately using a greedy algorithm
	void SolveJoinOrderApproximately();

	unique_ptr<LogicalOperator> ResolveJoinConditions(unique_ptr<LogicalOperator> op);
	std::pair<RelationSet *, unique_ptr<LogicalOperator>>
	GenerateJoins(vector<unique_ptr<LogicalOperator>> &extracted_relations, JoinNode *node);
};

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// optimizer/join_order_optimizer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/logical_operator.hpp"
#include "planner/logical_operator_visitor.hpp"
#include "optimizer/join_order/query_graph.hpp"
#include "optimizer/join_order/relation.hpp"

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

		//! Create a leaf node in the join tree
		JoinNode(RelationSet *set, size_t cardinality) : 
			set(set), info(nullptr), cardinality(cardinality), cost(cardinality), left(nullptr), right(nullptr) {
		}
		//! Create an intermediate node in the join tree
		JoinNode(RelationSet *set, NeighborInfo *info, JoinNode *left, JoinNode* right, size_t cardinality, size_t cost) : 
			set(set), info(info), cardinality(cardinality), cost(cost), left(left), right(right) {
		}
	};
public:
	//! Perform join reordering inside a plan
	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> plan);

	using LogicalOperatorVisitor::Visit;
	unique_ptr<Expression> Visit(SubqueryExpression &expr) override;
private:
	//! Set of all relations considered in the join optimizer
	vector<unique_ptr<Relation>> relations;
	//! A mapping of base table index -> index into relations array (relation number)
	unordered_map<size_t, size_t> relation_mapping;
	//! A structure holding all the created RelationSet objects
	RelationSetManager set_manager;
	//! The set of filters that we want to push down
	FilterNode pushdown_filters;
	//! The set of edges used in the join optimizer
	QueryGraph query_graph;
	//! The optimal join plan found for the specific RelationSet*
	unordered_map<RelationSet*, unique_ptr<JoinNode>> plans;

	// Add a filter to the set of to-be-pushed-down filters, where the filter needs columns from the given RelationSet to be evaluated
	void AddPushdownFilter(RelationSet *set, FilterInfo* filter);
	//! Enumerate all pushdown filters that can be fulfilled by a given RelationSet node (i.e. all entries in pushdown_filters where the given RelationSet is a subset of [[node]]). Return true in the callback to remove the filter from the set of pushdown filters.
	void EnumeratePushdownFilters(RelationSet *node, std::function<bool(FilterInfo*)> callback);

	//! Extract the bindings referred to by an Expression
	bool ExtractBindings(Expression &expression, std::unordered_set<size_t> &bindings);
	//! Traverse the query tree to find (1) base relations, (2) existing join conditions and (3) filters that can be rewritten into joins. Returns true if there are joins in the tree that can be reordered, false otherwise.
	bool ExtractJoinRelations(LogicalOperator &input_op, vector<unique_ptr<FilterInfo>>& filters, LogicalOperator *parent = nullptr);
	//! Emit a pair as a potential join candidate
	void EmitPair(RelationSet *left, RelationSet *right, NeighborInfo *info);

	void EnumerateCmpRecursive(RelationSet *left, RelationSet *right, std::unordered_set<size_t> exclusion_set);
	//! Emit a relation set node
	void EmitCSG(RelationSet *node);
	//! Enumerate the possible connected subgraphs that can be joined together in the join graph
	void EnumerateCSGRecursive(RelationSet *node, std::unordered_set<size_t> &exclusion_set);
	//! Rewrite a logical query plan given the join plan
	unique_ptr<LogicalOperator> RewritePlan(unique_ptr<LogicalOperator> plan, JoinNode* node);
	//! Generate cross product edges inside the side
	void GenerateCrossProducts();
	//! Perform the join order solving
	void SolveJoinOrder();

	unique_ptr<LogicalOperator> ResolveJoinConditions(unique_ptr<LogicalOperator> op);
	std::pair<RelationSet*, unique_ptr<LogicalOperator>> GenerateJoins(vector<unique_ptr<LogicalOperator>>& extracted_relations, JoinNode* node);
};

} // namespace duckdb

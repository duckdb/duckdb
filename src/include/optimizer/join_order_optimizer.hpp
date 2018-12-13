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

#include <functional>
#include <unordered_set>

namespace duckdb {

class JoinOrderOptimizer : public LogicalOperatorVisitor {
public:
	//! Represents a single relation and any metadata accompanying that relation
	struct Relation {
		LogicalOperator *op;
		LogicalOperator *parent;

		Relation() {}
		Relation(LogicalOperator *op, LogicalOperator *parent) : op(op), parent(parent) { }
	};

	//! Set of relations, used in the join graph
	struct RelationSet {
		RelationSet(unique_ptr<size_t[]> relations, size_t count) : 
			relations(move(relations)), count(count) {
		}

		string ToString() {
			string result = "[";
			for(size_t i = 0; i < count; i++) {
				result += std::to_string(relations[i]);
				if (i != count - 1) {
					result += ", ";
				}
			}
			result += "]";
			return result;
		}

		unique_ptr<size_t[]> relations;
		size_t count;
	};

	//! Contains a node with a RelationSet and child relations
	struct RelationInfo {
		unique_ptr<RelationSet> relation;
		unordered_map<size_t, RelationInfo> children;
	};

	struct FilterInfo {
		Expression *filter;
		LogicalOperator *parent;
		RelationSet *left_set;
		RelationSet *right_set;
	};

	struct FilterNode {
		vector<FilterInfo*> filters;
		unordered_map<size_t, FilterNode> children;
	};

	struct NeighborInfo {
		RelationSet* neighbor;
		vector<FilterInfo*> filters;
	};

	//! Contains a node with info about neighboring relations and child edge infos
	struct EdgeInfo {
		vector<unique_ptr<NeighborInfo>> neighbors;
		unordered_map<size_t, EdgeInfo> children;
	};

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
	//! A structure holding all the created RelationSet objects and allowing fast lookup on to them
	RelationInfo relation_set;
	//! The set of filters that we want to push down
	FilterNode pushdown_filters;
	//! The set of edges used in the join optimizer
	EdgeInfo edge_set;
	//! The optimal join plan found for the specific RelationSet*
	unordered_map<RelationSet*, unique_ptr<JoinNode>> plans;

	// Add a filter to the set of to-be-pushed-down filters, where the filter needs columns from the given RelationSet to be evaluated
	void AddPushdownFilter(RelationSet *set, FilterInfo* filter);
	//! Enumerate all pushdown filters that can be fulfilled by a given RelationSet node (i.e. all entries in pushdown_filters where the given RelationSet is a subset of [[node]]). Return true in the callback to remove the filter from the set of pushdown filters.
	void EnumeratePushdownFilters(RelationSet *node, std::function<bool(FilterInfo*)> callback);

	//! Union two sets of relations together and create a new relation set
	RelationSet* Union(RelationSet *left, RelationSet *right);
	//! Create the set difference of left \ right (i.e. all elements in left that are not in right)
	RelationSet* Difference(RelationSet *left, RelationSet *right);

	//! Extract the bindings referred to by an Expression
	bool ExtractBindings(Expression &expression, std::unordered_set<size_t> &bindings);
	//! Create an edge in the edge_set
	void CreateEdge(RelationSet *left, RelationSet *right, FilterInfo* info);
	//! Get the EdgeInfo of a specific node
	EdgeInfo* GetEdgeInfo(RelationSet *left);
	//! Create or get a RelationSet from a single node with the given index
	RelationSet *GetRelation(size_t index);
	//! Create or get a RelationSet from a set of relation bindings
	RelationSet* GetRelation(std::unordered_set<size_t> &bindings);
	//! Create or get a RelationSet from a (sorted, duplicate-free!) list of relations
	RelationSet *GetRelation(unique_ptr<size_t[]> relations, size_t count);
	//! Traverse the query tree to find (1) base relations, (2) existing join conditions and (3) filters that can be rewritten into joins. Returns true if there are joins in the tree that can be reordered, false otherwise.
	bool ExtractJoinRelations(LogicalOperator &input_op, vector<unique_ptr<FilterInfo>>& filters, LogicalOperator *parent = nullptr);
	//! Enumerate the neighbors of a specific node that do not belong to any of the exclusion_set. Note that if a neighbor has multiple nodes, this function will return the lowest entry in that set.
	vector<size_t> GetNeighbors(RelationSet *node, std::unordered_set<size_t> &exclusion_set);
	//! Enumerate all neighbors of a given RelationSet node
	void EnumerateNeighbors(RelationSet *node, std::function<bool(NeighborInfo*)> callback);
	//! Emit a pair as a potential join candidate
	void EmitPair(RelationSet *left, RelationSet *right, NeighborInfo *info);

	//! Returns a connection if there is an edge that connects these two sets, or nullptr otherwise
	NeighborInfo* GetConnection(RelationSet *node, RelationSet *other);
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

	std::pair<RelationSet*, unique_ptr<LogicalOperator>> GenerateJoins(vector<unique_ptr<LogicalOperator>>& extracted_relations, JoinNode* node);
};

} // namespace duckdb

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

class JoinOrderOptimizer {
public:
	//! Represents a single relation and any metadata accompanying that relation
	struct Relation {
		size_t index;
		LogicalOperator *op;
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

	//! Contains a node with info about neighboring relations and child edge infos
	struct EdgeInfo {
		vector<RelationSet*> neighbors;
		unordered_map<size_t, EdgeInfo> children;
	};

	//! Represents a node in the join plan
	struct JoinNode {
		RelationSet *set;
		size_t cardinality;
		size_t cost;
		JoinNode *left;
		JoinNode *right;

		//! Create a leaf node in the join tree
		JoinNode(RelationSet *set, size_t cardinality) : 
			set(set), cardinality(cardinality), cost(cardinality), left(nullptr), right(nullptr) {
		}
		//! Create an intermediate node in the join tree
		JoinNode(RelationSet *set, JoinNode *left, JoinNode* right, size_t cardinality, size_t cost) : 
			set(set), cardinality(cardinality), cost(cost), left(left), right(right) {
		}
	};
public:
	//! Perform join reordering inside a plan
	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> plan);
private:
	unordered_map<size_t, size_t> relation_mapping;
	unordered_map<size_t, Relation> relations;
	vector<Expression*> filters;
	unordered_map<size_t, RelationInfo> relation_set;
	unordered_map<size_t, EdgeInfo> edge_set;
	unordered_map<RelationSet*, unique_ptr<JoinNode>> plans;


	//! Extract the bindings referred to by an Expression
	void ExtractBindings(Expression &expression, std::unordered_set<size_t> &bindings);
	//! Create an edge in the edge_set
	void CreateEdge(RelationSet *left, RelationSet *right);
	//! Get the EdgeInfo of a specific node
	EdgeInfo* GetEdgeInfo(RelationSet *left);
	//! Union two sets of relations together and create a new relation set
	RelationSet* Union(RelationSet *left, RelationSet *right);
	//! Create or get a RelationSet from a single node with the given index
	RelationSet *GetRelation(size_t index);
	//! Create or get a RelationSet from a set of relation bindings
	RelationSet* GetRelation(std::unordered_set<size_t> &bindings);
	//! Create or get a RelationSet from a (sorted, duplicate-free!) list of relations
	RelationSet *GetRelation(unique_ptr<size_t[]> relations, size_t count);
	//! Traverse the query tree to find (1) base relations, (2) existing join conditions and (3) filters that can be rewritten into joins. Returns true if there are joins in the tree that can be reordered, false otherwise.
	bool ExtractJoinRelations(LogicalOperator &input_op);
	//! Enumerate the neighbors of a specific node that do not belong to any of the exclusion_set. Note that if a neighbor has multiple nodes, this function will return the lowest entry in that set.
	vector<size_t> GetNeighbors(RelationSet *node, std::unordered_set<size_t> &exclusion_set);

	void EnumerateNeighbors(RelationSet *node, std::function<bool(RelationSet*)> callback);
	//! Emit a pair as a potential join candidate
	void EmitPair(RelationSet *left, RelationSet *right);

	//! Returns true if there is an edge that connects these two sets
	bool IsConnected(RelationSet *node, RelationSet *other);
	void EnumerateCmpRecursive(RelationSet *left, RelationSet *right, std::unordered_set<size_t> exclusion_set);
	//! Emit a relation set node
	void EmitCSG(RelationSet *node);
	//! Enumerate the possible connected subgraphs that can be joined together in the join graph
	void EnumerateCSGRecursive(RelationSet *node, std::unordered_set<size_t> &exclusion_set);
};

} // namespace duckdb

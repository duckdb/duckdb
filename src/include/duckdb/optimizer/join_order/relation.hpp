//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/join_order/relation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"

namespace duckdb {
class LogicalOperator;

//! Represents a single relation and any metadata accompanying that relation
struct Relation {
	LogicalOperator *op;
	LogicalOperator *parent;

	Relation() {
	}
	Relation(LogicalOperator *op, LogicalOperator *parent) : op(op), parent(parent) {
	}
};

//! Set of relations, used in the join graph.
struct RelationSet {
	RelationSet(unique_ptr<idx_t[]> relations, idx_t count) : relations(move(relations)), count(count) {
	}

	string ToString() const;

	unique_ptr<idx_t[]> relations;
	idx_t count;

	static bool IsSubset(RelationSet *super, RelationSet *sub);
};

//! The RelationTree is a structure holding all the created RelationSet objects and allowing fast lookup on to them
class RelationSetManager {
public:
	//! Contains a node with a RelationSet and child relations
	// FIXME: this structure is inefficient, could use a bitmap for lookup instead (todo: profile)
	struct RelationTreeNode {
		unique_ptr<RelationSet> relation;
		unordered_map<idx_t, unique_ptr<RelationTreeNode>> children;
	};

public:
	//! Create or get a RelationSet from a single node with the given index
	RelationSet *GetRelation(idx_t index);
	//! Create or get a RelationSet from a set of relation bindings
	RelationSet *GetRelation(unordered_set<idx_t> &bindings);
	//! Create or get a RelationSet from a (sorted, duplicate-free!) list of relations
	RelationSet *GetRelation(unique_ptr<idx_t[]> relations, idx_t count);
	//! Union two sets of relations together and create a new relation set
	RelationSet *Union(RelationSet *left, RelationSet *right);
	//! Create the set difference of left \ right (i.e. all elements in left that are not in right)
	RelationSet *Difference(RelationSet *left, RelationSet *right);

private:
	RelationTreeNode root;
};

} // namespace duckdb

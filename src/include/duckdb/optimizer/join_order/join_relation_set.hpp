//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/join_order/join_relation_set.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/unordered_multimap.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/optimizer/join_order/relation_index.hpp"

namespace duckdb {

//! Set of relations, used in the join graph.
struct JoinRelationSet {
public:
	JoinRelationSet(unsafe_unique_array<RelationIndex> relations, idx_t count);

public:
	string ToString() const;
	bool Empty() const;
	bool Contains(RelationIndex relation) const;
	static bool IsSubset(const JoinRelationSet &super, const JoinRelationSet &sub);
	static bool Intersects(const JoinRelationSet &left, const JoinRelationSet &right);

public:
	unsafe_unique_array<RelationIndex> relations;
	idx_t count;
	//! Bitmask over the relation indexes for fast subset/intersection checks
	vector<idx_t> bitmask;

private:
	//! First word containing a relation (zero for empty sets)
	idx_t first_word = 0;
};

//! Owns and interns relation sets so equal sets always refer to the same object.
class JoinRelationSetManager {
public:
	//! Get an empty JoinRelationSet
	JoinRelationSet &GetEmptyJoinRelationSet();
	//! Create or get a JoinRelationSet from a single node with the given index
	JoinRelationSet &GetJoinRelation(RelationIndex index);
	//! Create or get a JoinRelationSet from a set of relation bindings
	JoinRelationSet &GetJoinRelation(const unordered_set<RelationIndex> &bindings);
	//! Create or get a JoinRelationSet from a (sorted, duplicate-free!) list of relations
	JoinRelationSet &GetJoinRelation(unsafe_unique_array<RelationIndex> relations, idx_t count);
	//! Union two sets of relations together and create a new relation set
	JoinRelationSet &Union(JoinRelationSet &left, JoinRelationSet &right);
	JoinRelationSet &Intersection(const JoinRelationSet &left, const JoinRelationSet &right);
	JoinRelationSet &Difference(const JoinRelationSet &left, const JoinRelationSet &right);
	string ToString() const;
	void Print();

private:
	//! Hash collisions retain distinct sets; entries own the stable relation-set objects.
	unordered_multimap<hash_t, unique_ptr<JoinRelationSet>> relation_sets;
	optional_ptr<JoinRelationSet> empty_relation_set;
};

} // namespace duckdb

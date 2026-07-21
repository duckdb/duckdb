//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/join_order/semantic_join_edge.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/optimizer/join_order/join_relation_set.hpp"
#include "duckdb/planner/joinside.hpp"

namespace duckdb {

//! A non-inner join that must be enumerated and reconstructed atomically.
struct SemanticJoinEdge {
	SemanticJoinEdge(idx_t index_p, JoinType join_type_p, JoinRelationSet &predicate_left_set_p,
	                 JoinRelationSet &predicate_right_set_p, vector<JoinCondition> conditions_p)
	    : index(index_p), join_type(join_type_p), predicate_left_set(predicate_left_set_p),
	      predicate_right_set(predicate_right_set_p), required_left_set(predicate_left_set_p),
	      required_right_set(predicate_right_set_p), conditions(std::move(conditions_p)) {
	}

	idx_t index;
	JoinType join_type;
	//! Relations referenced by the predicate. These remain binding-precise for costing.
	reference<JoinRelationSet> predicate_left_set;
	reference<JoinRelationSet> predicate_right_set;
	//! Predicate endpoints expanded only by nullable-producing semantic prerequisites.
	reference<JoinRelationSet> required_left_set;
	reference<JoinRelationSet> required_right_set;
	//! Semantic edges that must have been reconstructed in a child of this edge.
	vector<idx_t> prerequisite_edges;
	vector<JoinCondition> conditions;
	vector<idx_t> costing_predicate_indices;
};

} // namespace duckdb

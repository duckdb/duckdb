//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/join_order/non_inner_join_edge.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/optimizer/join_order/join_relation_set.hpp"
#include "duckdb/planner/joinside.hpp"

namespace duckdb {

//! A reorderable non-inner join that owns its complete condition throughout join enumeration.
struct NonInnerJoinEdge {
	NonInnerJoinEdge(idx_t index_p, JoinType join_type_p, JoinRelationSet &predicate_left_set_p,
	                 JoinRelationSet &predicate_right_set_p, vector<JoinCondition> conditions_p)
	    : index(index_p), join_type(join_type_p), predicate_left_set(predicate_left_set_p),
	      predicate_right_set(predicate_right_set_p), required_left_set(predicate_left_set_p),
	      required_right_set(predicate_right_set_p), conditions(std::move(conditions_p)) {
	}

	idx_t index;
	JoinType join_type;
	//! Relations referenced by the condition before prerequisite expansion.
	reference<JoinRelationSet> predicate_left_set;
	reference<JoinRelationSet> predicate_right_set;
	//! Inputs required to reconstruct this edge after prerequisite expansion.
	reference<JoinRelationSet> required_left_set;
	reference<JoinRelationSet> required_right_set;
	//! Edges that must be present in one of this edge's input subtrees.
	vector<idx_t> prerequisite_edges;
	//! The complete normalized join condition, owned by this edge until reconstruction.
	vector<JoinCondition> conditions;
	//! Predicate copies used only by cardinality estimation and costing.
	vector<idx_t> costing_predicate_indices;
};

} // namespace duckdb

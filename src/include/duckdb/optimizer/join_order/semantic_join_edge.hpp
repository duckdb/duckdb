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
	SemanticJoinEdge(idx_t index_p, JoinType join_type_p, JoinRelationSet &left_set_p, JoinRelationSet &right_set_p,
	                 vector<JoinCondition> conditions_p)
	    : index(index_p), join_type(join_type_p), left_set(left_set_p), right_set(right_set_p),
	      conditions(std::move(conditions_p)) {
	}

	idx_t index;
	JoinType join_type;
	reference<JoinRelationSet> left_set;
	reference<JoinRelationSet> right_set;
	vector<JoinCondition> conditions;
	vector<idx_t> costing_predicate_indices;
};

} // namespace duckdb

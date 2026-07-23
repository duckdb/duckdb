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
#include "duckdb/planner/joinside.hpp"

namespace duckdb {

//! A reorderable non-inner join that owns its complete condition throughout join enumeration.
struct NonInnerJoinEdge {
	NonInnerJoinEdge(idx_t index_p, JoinType join_type_p, vector<JoinCondition> conditions_p)
	    : index(index_p), join_type(join_type_p), conditions(std::move(conditions_p)) {
	}

	idx_t index;
	JoinType join_type;
	//! The complete normalized join condition, owned by this edge until reconstruction.
	vector<JoinCondition> conditions;
	//! Predicate copies used only by cardinality estimation and costing.
	vector<idx_t> costing_predicate_indices;
};

} // namespace duckdb

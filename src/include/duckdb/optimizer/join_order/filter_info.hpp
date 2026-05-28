//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/join_order/filter_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/optimizer/join_order/join_relation_set.hpp"
#include "duckdb/planner/column_binding.hpp"

namespace duckdb {

//! Filter info struct that is used by the cardinality estimator to set the initial cardinality
//! but is also eventually transformed into a query edge.
class FilterInfo {
public:
	FilterInfo(unique_ptr<Expression> filter, JoinRelationSet &set, idx_t filter_index,
	           JoinType join_type = JoinType::INNER);
	~FilterInfo();

public:
	void SetLeftSet(optional_ptr<JoinRelationSet> left_set_new);
	void SetRightSet(optional_ptr<JoinRelationSet> right_set_new);

public:
	unique_ptr<Expression> filter;
	reference<JoinRelationSet> set;
	idx_t filter_index;
	JoinType join_type;
	optional_ptr<JoinRelationSet> left_set;
	optional_ptr<JoinRelationSet> right_set;
	ColumnBinding left_binding;
	ColumnBinding right_binding;
	bool from_residual_predicate = false;
	//! Index of the equivalence group for INNER equality/IS NOT DISTINCT FROM join filters.
	//! All filters transitively connected by equality (a=b, b=c -> a=c all share the same index).
	//! Used by cardinality estimation to skip redundant transitive conditions.
	optional_idx edge_equivalence_index;
};

} // namespace duckdb

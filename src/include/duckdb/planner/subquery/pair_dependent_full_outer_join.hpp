//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/subquery/pair_dependent_full_outer_join.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/subquery/recursive_dependent_join_planner.hpp"

namespace duckdb {

//! Plans the match-domain rewrite for pair-dependent FULL OUTER JOIN predicates.
class PairDependentFullOuterJoinPlanner {
public:
	static LogicalRewriteResult Plan(Binder &binder, unique_ptr<Expression> condition, unique_ptr<LogicalOperator> left,
	                                 unique_ptr<LogicalOperator> right, const unordered_set<TableIndex> &left_bindings,
	                                 const unordered_set<TableIndex> &right_bindings);
};

} // namespace duckdb

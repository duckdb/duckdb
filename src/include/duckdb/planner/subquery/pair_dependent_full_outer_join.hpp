//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/subquery/pair_dependent_full_outer_join.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unordered_set.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"

namespace duckdb {

class Binder;

struct PairDependentJoinPlan {
	unique_ptr<LogicalOperator> plan;
	BindingReplacementMap output_replacements;
};

//! Plans the match-domain rewrite for pair-dependent FULL OUTER JOIN predicates.
class PairDependentFullOuterJoinPlanner {
public:
	static PairDependentJoinPlan Plan(Binder &binder, unique_ptr<Expression> condition,
	                                  unique_ptr<LogicalOperator> left, unique_ptr<LogicalOperator> right,
	                                  const unordered_set<TableIndex> &left_bindings,
	                                  const unordered_set<TableIndex> &right_bindings);
};

} // namespace duckdb

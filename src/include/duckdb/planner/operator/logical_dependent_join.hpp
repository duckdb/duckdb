//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_dependent_join.hpp
//
// logical_dependent_join represents a logical operator for lateral joins that
// is planned but not yet flattened
//
// This construct only exists during planning and should not exist in the plan
// once flattening is complete. Although the same information can be kept in the
// join itself, creating a new construct makes the code cleaner and easier to
// understand.
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_join.hpp"

namespace duckdb {

class LogicalDependentJoin : public LogicalJoin {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_DEPENDENT_JOIN;

public:
	explicit LogicalDependentJoin(JoinType type);

	//! The condition of the dependent join, if any
	unique_ptr<Expression> condition;
	//! The list of columns that have correlations with the right
	CorrelatedColumns correlated_columns;

	bool perform_delim = true;
	bool any_join = false;
	bool propagate_null_values = true;
};
} // namespace duckdb

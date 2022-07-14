//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/filter_pullup.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unordered_set.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

class FilterPullup {
public:
	explicit FilterPullup(bool pullup = false, bool add_column = false)
	    : can_pullup(pullup), can_add_column(add_column) {
	}

	//! Perform filter pullup
	unique_ptr<LogicalOperator> Rewrite(unique_ptr<LogicalOperator> op);

private:
	vector<unique_ptr<Expression>> filters_expr_pullup;

	// only pull up filters when there is a fork
	bool can_pullup = false;

	// identifiy case the branch is a set operation (INTERSECT or EXCEPT)
	bool can_add_column = false;

private:
	// Generate logical filters pulled up
	unique_ptr<LogicalOperator> GeneratePullupFilter(unique_ptr<LogicalOperator> child,
	                                                 vector<unique_ptr<Expression>> &expressions);

	//! Pull up a LogicalFilter op
	unique_ptr<LogicalOperator> PullupFilter(unique_ptr<LogicalOperator> op);

	//! Pull up filter in a LogicalProjection op
	unique_ptr<LogicalOperator> PullupProjection(unique_ptr<LogicalOperator> op);

	//! Pull up filter in a LogicalCrossProduct op
	unique_ptr<LogicalOperator> PullupCrossProduct(unique_ptr<LogicalOperator> op);

	unique_ptr<LogicalOperator> PullupJoin(unique_ptr<LogicalOperator> op);

	// PPullup filter in a left join
	unique_ptr<LogicalOperator> PullupFromLeft(unique_ptr<LogicalOperator> op);

	// Pullup filter in a inner join
	unique_ptr<LogicalOperator> PullupInnerJoin(unique_ptr<LogicalOperator> op);

	// Pullup filter in LogicalIntersect or LogicalExcept op
	unique_ptr<LogicalOperator> PullupSetOperation(unique_ptr<LogicalOperator> op);

	unique_ptr<LogicalOperator> PullupBothSide(unique_ptr<LogicalOperator> op);

	// Finish pull up at this operator
	unique_ptr<LogicalOperator> FinishPullup(unique_ptr<LogicalOperator> op);

	// special treatment for SetOperations and projections
	void ProjectSetOperation(LogicalProjection &proj);

}; // end FilterPullup

} // namespace duckdb

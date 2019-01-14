//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/operator/logical_filter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/logical_operator.hpp"

namespace duckdb {

//! LogicalFilter represents a filter operation (e.g. WHERE or HAVING clause)
class LogicalFilter : public LogicalOperator {
public:
	LogicalFilter(unique_ptr<Expression> expression);
	LogicalFilter();

	vector<string> GetNames() override;
	bool SplitPredicates() {
		return SplitPredicates(expressions);
	}
	//! Splits up the predicates of the LogicalFilter into a set of predicates
	//! separated by AND Returns whether or not any splits were made
	static bool SplitPredicates(vector<unique_ptr<Expression>> &expressions);

	//! True if the filter is guaranteed to produce an empty result. This flag is set by the optimizer
	bool empty_result = false;

protected:
	void ResolveTypes() override;
};

} // namespace duckdb

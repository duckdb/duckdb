//===----------------------------------------------------------------------===//
//                         DuckDB
//
// optimizer/filter_pushdown.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/rule.hpp"

#include <unordered_set>

namespace duckdb {

class FilterPushdown {
public:
	//! Perform filter pushdown
	unique_ptr<LogicalOperator> Rewrite(unique_ptr<LogicalOperator> node);

	struct Filter {
		unordered_set<size_t> bindings;
		unique_ptr<Expression> filter;

		Filter(){}
		Filter(unique_ptr<Expression> filter) : filter(move(filter)) { }
	};

private:
	vector<unique_ptr<Filter>> filters;

	//! Push down a LogicalFilter op
	unique_ptr<LogicalOperator> PushdownFilter(unique_ptr<LogicalOperator> op);
	unique_ptr<LogicalOperator> PushdownCrossProduct(unique_ptr<LogicalOperator> op);
	unique_ptr<LogicalOperator> PushdownJoin(unique_ptr<LogicalOperator> op);
	unique_ptr<LogicalOperator> PushdownSubquery(unique_ptr<LogicalOperator> op);


	unique_ptr<LogicalOperator> PushdownInnerJoin(unique_ptr<LogicalOperator> op,
                                                             unordered_set<size_t> &left_bindings,
                                                             unordered_set<size_t> &right_bindings);
	unique_ptr<LogicalOperator> PushdownLeftJoin(unique_ptr<LogicalOperator> op,
	                                             unordered_set<size_t> &left_bindings,
	                                             unordered_set<size_t> &right_bindings);
	unique_ptr<LogicalOperator> PushdownMarkJoin(unique_ptr<LogicalOperator> op,
                                                             unordered_set<size_t> &left_bindings,
                                                             unordered_set<size_t> &right_bindings);
	unique_ptr<LogicalOperator> PushdownSingleJoin(unique_ptr<LogicalOperator> op,
                                                             unordered_set<size_t> &left_bindings,
                                                             unordered_set<size_t> &right_bindings);

	unique_ptr<LogicalOperator> FinishPushdown(unique_ptr<LogicalOperator> op);
};

} // namespace duckdb

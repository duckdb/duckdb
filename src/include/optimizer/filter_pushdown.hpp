//===----------------------------------------------------------------------===//
//                         DuckDB
//
// optimizer/filter_pushdown.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/expression_rewriter.hpp"
#include "optimizer/rule.hpp"

#include <unordered_set>

namespace duckdb {

class FilterPushdown {
public:
	FilterPushdown(ExpressionRewriter& rewriter) : 
		rewriter(rewriter) {}
	//! Perform filter pushdown
	unique_ptr<LogicalOperator> Rewrite(unique_ptr<LogicalOperator> node);

	struct Filter {
		unordered_set<size_t> bindings;
		unique_ptr<Expression> filter;

		Filter(){}
		Filter(unique_ptr<Expression> filter) : filter(move(filter)) { }

		void ExtractBindings();
	};

private:
	vector<unique_ptr<Filter>> filters;
	ExpressionRewriter& rewriter;

	//! Push down a LogicalAggregate op
	unique_ptr<LogicalOperator> PushdownAggregate(unique_ptr<LogicalOperator> op);
	//! Push down a LogicalFilter op
	unique_ptr<LogicalOperator> PushdownFilter(unique_ptr<LogicalOperator> op);
	//! Push down a LogicalCrossProduct op
	unique_ptr<LogicalOperator> PushdownCrossProduct(unique_ptr<LogicalOperator> op);
	//! Push down a join operator
	unique_ptr<LogicalOperator> PushdownJoin(unique_ptr<LogicalOperator> op);
	//! Push down a LogicalSubquery op
	unique_ptr<LogicalOperator> PushdownSubquery(unique_ptr<LogicalOperator> op);
	//! Push down a LogicalProjection op
	unique_ptr<LogicalOperator> PushdownProjection(unique_ptr<LogicalOperator> op);
	//! Push down a LogicalSetOperation op
	unique_ptr<LogicalOperator> PushdownSetOperation(unique_ptr<LogicalOperator> op);
	

	// Pushdown an inner join
	unique_ptr<LogicalOperator> PushdownInnerJoin(unique_ptr<LogicalOperator> op,
                                                             unordered_set<size_t> &left_bindings,
                                                             unordered_set<size_t> &right_bindings);
	// Pushdown a left join
	unique_ptr<LogicalOperator> PushdownLeftJoin(unique_ptr<LogicalOperator> op,
	                                             unordered_set<size_t> &left_bindings,
	                                             unordered_set<size_t> &right_bindings);
	// Pushdown a mark join
	unique_ptr<LogicalOperator> PushdownMarkJoin(unique_ptr<LogicalOperator> op,
                                                             unordered_set<size_t> &left_bindings,
                                                             unordered_set<size_t> &right_bindings);
	// Pushdown a single join
	unique_ptr<LogicalOperator> PushdownSingleJoin(unique_ptr<LogicalOperator> op,
                                                             unordered_set<size_t> &left_bindings,
                                                             unordered_set<size_t> &right_bindings);

	// Finish pushing down at this operator, creating a LogicalFilter to store any of the stored filters and recursively pushing down into its children (if any)
	unique_ptr<LogicalOperator> FinishPushdown(unique_ptr<LogicalOperator> op);
	//! Adds a filter to the set of filters. Returns true if the filter statically evaluates to false and the entire tree should be stripped.
	bool AddFilter(unique_ptr<Expression> expr);
};

} // namespace duckdb

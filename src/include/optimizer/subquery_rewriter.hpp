//===----------------------------------------------------------------------===//
//                         DuckDB
//
// optimizer/subquery_rewriter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/logical_operator.hpp"

namespace duckdb {

class BindContext;

//! The subquery rewriter is in charge of moving subqueries inside of expressions into joins with the main plan (i.e.
//! flattening the plan)
class SubqueryRewriter {
public:
	SubqueryRewriter(BindContext &context) : context(context) {
	}

	//! Flatten a plan with (potential) subqueries into a normal plan
	unique_ptr<LogicalOperator> Rewrite(unique_ptr<LogicalOperator> plan);

private:
	BindContext &context;

	//! Rewrite a (NOT) IN [SUBQUERY] clause
	bool RewriteInClause(LogicalFilter &filter, OperatorExpression *expression, BoundSubqueryExpression *subquery);
	//! Rewrite a (NOT) EXISTS [SUBQUERY] clause
	bool RewriteExistsClause(LogicalFilter &filter, OperatorExpression *expression, BoundSubqueryExpression *subquery);
	//! Rewrite a comparison with a subquery (e.g. A == [SUBQUERY])
	bool RewriteSubqueryComparison(LogicalFilter &filter, ComparisonExpression *exists,
	                               BoundSubqueryExpression *subquery);
};

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// optimizer/expression_rewriter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/rule.hpp"

namespace duckdb {

//! The ExpressionRewriter performs a set of fixed rewrite rules on the expressions that occur in a SQL statement
class ExpressionRewriter {
public:
	//! The set of rules as known by the Expression Rewriter
	vector<unique_ptr<Rule>> rules;

	//! Apply the rules to a specific LogicalOperator
	void Apply(LogicalOperator &root);

private:
	//! Apply a set of rules to a specific expression
	static unique_ptr<Expression> ApplyRules(LogicalOperator &op, const vector<Rule *> &rules,
	                                         unique_ptr<Expression> expr);
};

} // namespace duckdb

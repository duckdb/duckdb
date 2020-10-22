//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/expression_rewriter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/rule.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {
class ClientContext;

//! The ExpressionRewriter performs a set of fixed rewrite rules on the expressions that occur in a SQL statement
class ExpressionRewriter {
public:
	ExpressionRewriter(ClientContext &context) : context(context) {
	}
	//! The set of rules as known by the Expression Rewriter
	vector<unique_ptr<Rule>> rules;

	ClientContext &context;

public:
	//! Apply the rules to a specific LogicalOperator
	void Apply(LogicalOperator &root);

	// Generates either a constant_or_null(child) expression
	static unique_ptr<Expression> ConstantOrNull(unique_ptr<Expression> child, Value value);
private:
	//! Apply a set of rules to a specific expression
	static unique_ptr<Expression> ApplyRules(LogicalOperator &op, const vector<Rule *> &rules,
	                                         unique_ptr<Expression> expr, bool &changes_made);
};

} // namespace duckdb

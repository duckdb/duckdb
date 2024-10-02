//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/rule.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/matcher/expression_matcher.hpp"
#include "duckdb/optimizer/matcher/logical_operator_matcher.hpp"

namespace duckdb {
class ExpressionRewriter;

class Rule {
public:
	explicit Rule(ExpressionRewriter &rewriter) : rewriter(rewriter) {
	}
	virtual ~Rule() {
	}

	//! The expression rewriter this rule belongs to
	ExpressionRewriter &rewriter;
	//! The expression matcher of the rule
	unique_ptr<ExpressionMatcher> root;

	ClientContext &GetContext() const;
	virtual unique_ptr<Expression> Apply(LogicalOperator &op, vector<reference<Expression>> &bindings,
	                                     bool &fixed_point, bool is_root) = 0;
};

} // namespace duckdb

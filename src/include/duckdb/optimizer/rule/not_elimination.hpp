//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/rule/not_elimination.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/rule.hpp"

namespace duckdb {

// Eliminate the NOT operator expression with following rules.
// 1. NOT NOT Expression ==>  Expression
// 2. NOT IS_NOT_NULL/IS_NULL ==> IS_NULL/IS_NOT_NULL
// 3. NOT (Expression1 AND Expression2) ==> !Expression1 OR !Expression2
// 4. NOT (Expression1 OR Expression2) ==> !Expression1 AND !Expression2
class NotEliminationRule : public Rule {
public:
	explicit NotEliminationRule(ExpressionRewriter &rewriter);

	unique_ptr<Expression> Apply(LogicalOperator &op, vector<reference<Expression>> &bindings, bool &changes_made,
	                             bool is_root) override;

private:
	unique_ptr<Expression> NegateExpression(const Expression &expr);
};

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/rule/like_optimizations.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/rule.hpp"
#include "duckdb/function/scalar/string_functions.hpp"

namespace duckdb {

// The Like Optimization rule rewrites LIKE to optimized scalar functions (e.g.: prefix, suffix, and contains)
class LikeOptimizationRule : public Rule {
public:
	explicit LikeOptimizationRule(ExpressionRewriter &rewriter);

	unique_ptr<Expression> Apply(LogicalOperator &op, vector<reference<Expression>> &bindings, bool &changes_made,
	                             bool is_root) override;

	unique_ptr<Expression> ApplyRule(BoundFunctionExpression &expr, ScalarFunction function, string pattern,
	                                 bool is_not_like);
};

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/rule/like_optimizations.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>

#include "duckdb/optimizer/rule.hpp"
#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {
class BoundFunctionExpression;
class ExpressionRewriter;
class LogicalOperator;
class ScalarFunction;

class RegexOptimizationRule : public Rule {
public:
	explicit RegexOptimizationRule(ExpressionRewriter &rewriter);

	unique_ptr<Expression> Apply(LogicalOperator &op, vector<reference<Expression>> &bindings, bool &changes_made,
	                             bool is_root) override;

	unique_ptr<Expression> ApplyRule(BoundFunctionExpression *expr, ScalarFunction function, string pattern,
	                                 bool is_not_like);
};

} // namespace duckdb

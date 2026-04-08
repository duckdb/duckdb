//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/rule/arithmetic_simplification.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/rule.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {
class ExpressionRewriter;
class LogicalOperator;

// The Arithmetic Simplification rule applies arithmetic expressions to which the answer is known (e.g. X + 0 => X, X *
// 0 => 0)
class ArithmeticSimplificationRule : public Rule {
public:
	explicit ArithmeticSimplificationRule(ExpressionRewriter &rewriter);

	unique_ptr<Expression> Apply(LogicalOperator &op, vector<reference<Expression>> &bindings, bool &changes_made,
	                             bool is_root) override;
};

} // namespace duckdb

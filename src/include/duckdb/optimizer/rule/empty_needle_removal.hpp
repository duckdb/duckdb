//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/rule/empty_needle_removal.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/rule.hpp"

namespace duckdb {

// The Empty_needle_removal Optimization rule folds some foldable ConstantExpression
//(e.g.: PREFIX('xyz', '') is TRUE, PREFIX(NULL, '') is NULL, so rewrite PREFIX(x, '') to TRUE_OR_NULL(x)
class EmptyNeedleRemovalRule : public Rule {
public:
	explicit EmptyNeedleRemovalRule(ExpressionRewriter &rewriter);

	unique_ptr<Expression> Apply(LogicalOperator &op, vector<reference<Expression>> &bindings, bool &changes_made,
	                             bool is_root) override;
};

} // namespace duckdb

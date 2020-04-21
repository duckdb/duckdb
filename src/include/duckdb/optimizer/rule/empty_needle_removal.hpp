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

// The Like Optimization rule rewrites LIKE to optimized scalar functions (e.g.: prefix, suffix, and contains)
class EmptyNeedleRemovalRule : public Rule {
public:
	EmptyNeedleRemovalRule(ExpressionRewriter &rewriter);

	unique_ptr<Expression> Apply(LogicalOperator &op, vector<Expression *> &bindings, bool &changes_made) override;
};

} // namespace duckdb

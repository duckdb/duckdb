//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/rule/empty_prefix_removal.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/rule.hpp"
#include "duckdb/function/scalar/string_functions.hpp"

namespace duckdb {

// The Like Optimization rule rewrites LIKE to optimized scalar functions (e.g.: prefix, suffix, and contains)
class EmptyPrefixRemovalRule : public Rule {
public:
	EmptyPrefixRemovalRule(ExpressionRewriter &rewriter);

	unique_ptr<Expression> Apply(LogicalOperator &op, vector<Expression *> &bindings, bool &changes_made) override;
};

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/rule/comparison_simplification.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/rule.hpp"

namespace duckdb {

// The Comparison Simplification rule rewrites comparisons with a constant NULL (i.e. [x = NULL] => [NULL])
class ComparisonSimplificationRule : public Rule {
public:
	explicit ComparisonSimplificationRule(ExpressionRewriter &rewriter);

	unique_ptr<Expression> Apply(LogicalOperator &op, vector<reference<Expression>> &bindings, bool &changes_made,
	                             bool is_root) override;
};

} // namespace duckdb

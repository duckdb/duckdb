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
	ComparisonSimplificationRule(ExpressionRewriter &rewriter);

	unique_ptr<Expression> Apply(LogicalOperator &op, vector<Expression *> &bindings, bool &changes_made) override;
};

} // namespace duckdb

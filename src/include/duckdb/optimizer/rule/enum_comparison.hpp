//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/rule/enum_comparison.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/rule.hpp"

namespace duckdb {

// The Enum Comparison rule rewrites cases where two Enums are compared on an equality check
class EnumComparisonRule : public Rule {
public:
	explicit EnumComparisonRule(ExpressionRewriter &rewriter);

	unique_ptr<Expression> Apply(LogicalOperator &op, vector<reference<Expression>> &bindings, bool &changes_made,
	                             bool is_root) override;
};

} // namespace duckdb

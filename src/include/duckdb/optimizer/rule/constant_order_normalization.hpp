//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/rule/constant_order_normalization.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/rule.hpp"

namespace duckdb {

// Move constant expression parameters to the left in expression(i.e. x + 2 + y + 2 => 2 + 2 + x + y)
// for convenience of other rules(i.e. ConstantFoldingRule).
class ConstantOrderNormalizationRule : public Rule {
public:
	explicit ConstantOrderNormalizationRule(ExpressionRewriter &rewriter);

	unique_ptr<Expression> Apply(LogicalOperator &op, vector<reference<Expression>> &bindings, bool &changes_made,
	                             bool is_root) override;
};

} // namespace duckdb

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

// Group constant expression parameters together in commutative arithmetic expressions to expose
// constant folding opportunities. After folding collapses multiplication constants into a single
// value, keep that constant on the right to match the canonical binary operator shape.
class ConstantOrderNormalizationRule : public Rule {
public:
	explicit ConstantOrderNormalizationRule(ExpressionRewriter &rewriter);

	unique_ptr<Expression> Apply(LogicalOperator &op, vector<reference<Expression>> &bindings, bool &changes_made,
	                             bool is_root) override;
};

} // namespace duckdb

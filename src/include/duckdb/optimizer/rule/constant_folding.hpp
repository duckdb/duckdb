//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/rule/constant_folding.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/rule.hpp"

namespace duckdb {

// Fold any constant scalar expressions into a single constant (i.e. [2 + 2] => [4], [2 = 2] => [True], etc...)
class ConstantFoldingRule : public Rule {
public:
	ConstantFoldingRule(ExpressionRewriter &rewriter);

	unique_ptr<Expression> Apply(LogicalOperator &op, vector<Expression *> &bindings, bool &changes_made) override;
};

} // namespace duckdb

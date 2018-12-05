//===----------------------------------------------------------------------===//
//                         DuckDB
//
// optimizer/expression_rules/constant_folding.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/rule.hpp"

namespace duckdb {

class ConstantFoldingRule : public Rule {
public:
	ConstantFoldingRule();

	unique_ptr<Expression> Apply(Rewriter &rewriter, Expression &root, vector<AbstractOperator> &bindings,
	                             bool &fixed_point);
};

} // namespace duckdb

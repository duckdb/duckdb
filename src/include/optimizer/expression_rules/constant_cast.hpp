//===----------------------------------------------------------------------===//
//                         DuckDB
//
// optimizer/expression_rules/constant_cast.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/rule.hpp"

namespace duckdb {

class ConstantCastRule : public Rule {
public:
	ConstantCastRule();

	unique_ptr<Expression> Apply(Rewriter &rewriter, Expression &root, vector<AbstractOperator> &bindings,
	                             bool &fixed_point);
};

} // namespace duckdb

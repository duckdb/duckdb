//===----------------------------------------------------------------------===//
//                         DuckDB
//
// optimizer/rule/constant_fold.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/rule.hpp"

namespace duckdb {

class ConstantFoldingRule : public Rule {
public:
	ConstantFoldingRule();

	unique_ptr<Expression> Apply(vector<Expression*> &bindings, bool &fixed_point) override;
};

} // namespace duckdb

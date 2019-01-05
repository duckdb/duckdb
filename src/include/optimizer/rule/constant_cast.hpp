//===----------------------------------------------------------------------===//
//                         DuckDB
//
// optimizer/rule/constant_cast.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/rule.hpp"

namespace duckdb {

class ConstantCastRule : public Rule {
public:
	ConstantCastRule();

	unique_ptr<Expression> Apply(vector<Expression*> &bindings, bool &fixed_point) override;
};

} // namespace duckdb

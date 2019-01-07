//===----------------------------------------------------------------------===//
//                         DuckDB
//
// optimizer/rule/constant_folding.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/rule.hpp"

namespace duckdb {

// X + Y => Z
class ConstantFoldingRule : public Rule {
public:
	ConstantFoldingRule();

	unique_ptr<Expression> Apply(LogicalOperator &op, vector<Expression *> &bindings, bool &changes_made) override;
};

} // namespace duckdb

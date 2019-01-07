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

// CAST(X) => X
class ConstantCastRule : public Rule {
public:
	ConstantCastRule();

	unique_ptr<Expression> Apply(LogicalOperator &op, vector<Expression *> &bindings, bool &changes_made) override;
};

} // namespace duckdb

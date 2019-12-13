//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/rule/distributivity.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/rule.hpp"

namespace duckdb {

// (X AND B) OR (X AND C) OR (X AND D) = X AND (B OR C OR D)
class DistributivityRule : public Rule {
public:
	DistributivityRule(ExpressionRewriter &rewriter);

	unique_ptr<Expression> Apply(LogicalOperator &op, vector<Expression *> &bindings, bool &changes_made) override;
};

} // namespace duckdb

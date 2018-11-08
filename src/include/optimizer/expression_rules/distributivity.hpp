//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// optimizer/expression_rules/distributivity.hpp
//
// Author: Hannes Mühleisen & Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/rule.hpp"

namespace duckdb {

// (X AND B) OR (X AND C) OR (X AND D) = X AND (B OR C OR D)
class DistributivityRule : public Rule {
  public:
	DistributivityRule();

	std::unique_ptr<Expression> Apply(Rewriter &rewriter, Expression &root,
	                                  std::vector<AbstractOperator> &bindings,
	                                  bool &fixed_point);
};

} // namespace duckdb

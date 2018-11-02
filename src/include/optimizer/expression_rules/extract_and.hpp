//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// optimizer/expression_rules/extract_and.hpp
//
// Author: Hannes Mühleisen & Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/rule.hpp"

namespace duckdb {

// (X AND B) OR (X AND C) = X AND (B OR C)
class ExtractAndRule : public Rule {
  public:
	ExtractAndRule();

	std::unique_ptr<Expression> Apply(Rewriter &rewriter, Expression &root,
	                                  std::vector<AbstractOperator> &bindings);
};

} // namespace duckdb

//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// optimizer/logical_rules/cross_product_rewrite.hpp
//
// Author: Hannes Mühleisen & Mark Raasveldt
//
//===----------------------------------------------------------------------===//

// TODO: possibly merge into cross product rewrite, lots of overlap?

#pragma once

#include "optimizer/rule.hpp"

namespace duckdb {

class SelectionPushdownRule : public Rule {
  public:
	SelectionPushdownRule();

	std::unique_ptr<LogicalOperator>
	Apply(Rewriter &rewriter, LogicalOperator &root,
	      std::vector<AbstractOperator> &bindings);
};

} // namespace duckdb

//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// optimizer/logical_rules/in_clause_rewrite.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/rule.hpp"

namespace duckdb {

class InClauseRewriteRule : public Rule {
  public:
	InClauseRewriteRule();

	std::unique_ptr<LogicalOperator>
	Apply(Rewriter &rewriter, LogicalOperator &op_root,
	      std::vector<AbstractOperator> &bindings, bool &fixed_point);
};

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// optimizer/logical_rules/in_clause_rewrite.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/rule.hpp"

namespace duckdb {

class InClauseRewriteRule : public Rule {
public:
	InClauseRewriteRule();

	unique_ptr<LogicalOperator> Apply(Rewriter &rewriter, LogicalOperator &op_root, vector<AbstractOperator> &bindings,
	                                  bool &fixed_point);
};

} // namespace duckdb

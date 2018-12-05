//===----------------------------------------------------------------------===//
//                         DuckDB
//
// optimizer/logical_rules/exists_rewrite.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/rule.hpp"

namespace duckdb {

class ExistsRewriteRule : public Rule {
public:
	ExistsRewriteRule();

	unique_ptr<LogicalOperator> Apply(Rewriter &rewriter, LogicalOperator &op_root, vector<AbstractOperator> &bindings,
	                                  bool &fixed_point);
};

} // namespace duckdb

//===----------------------------------------------------------------------===// 
// 
//                         DuckDB 
// 
// optimizer/logical_rules/exists_rewrite.hpp
// 
// 
// 
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/rule.hpp"

namespace duckdb {

class ExistsRewriteRule : public Rule {
  public:
	ExistsRewriteRule();

	std::unique_ptr<LogicalOperator>
	Apply(Rewriter &rewriter, LogicalOperator &op_root,
	      std::vector<AbstractOperator> &bindings, bool &fixed_point);
};

} // namespace duckdb

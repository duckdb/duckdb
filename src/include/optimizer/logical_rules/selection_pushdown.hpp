//===----------------------------------------------------------------------===// 
// 
//                         DuckDB 
// 
// optimizer/logical_rules/selection_pushdown.hpp
// 
// 
// 
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/rule.hpp"

namespace duckdb {

class SelectionPushdownRule : public Rule {
  public:
	SelectionPushdownRule();

	std::unique_ptr<LogicalOperator>
	Apply(Rewriter &rewriter, LogicalOperator &root,
	      std::vector<AbstractOperator> &bindings, bool &fixed_point);
};

} // namespace duckdb

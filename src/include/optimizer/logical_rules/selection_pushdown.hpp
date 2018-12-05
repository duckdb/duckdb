//===----------------------------------------------------------------------===//
//                         DuckDB
//
// optimizer/logical_rules/selection_pushdown.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/rule.hpp"

namespace duckdb {

class SelectionPushdownRule : public Rule {
public:
	SelectionPushdownRule();

	unique_ptr<LogicalOperator> Apply(Rewriter &rewriter, LogicalOperator &root, vector<AbstractOperator> &bindings,
	                                  bool &fixed_point);
};

} // namespace duckdb

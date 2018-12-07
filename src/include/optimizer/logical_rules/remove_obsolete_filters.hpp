//===----------------------------------------------------------------------===//
//                         DuckDB
//
// optimizer/logical_rules/remove_obsolete_filters.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/rule.hpp"

namespace duckdb {
class RemoveObsoleteFilterRule : public Rule {
public:
	RemoveObsoleteFilterRule();

	unique_ptr<LogicalOperator> Apply(Rewriter &rewriter, LogicalOperator &op_root, vector<AbstractOperator> &bindings,
	                                  bool &fixed_point);
};
} // namespace duckdb

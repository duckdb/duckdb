//===----------------------------------------------------------------------===//
//                         DuckDB
//
// optimizer/logical_rules/split_filter_conjunction.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/rule.hpp"

namespace duckdb {

//! Splits up AND expressions in a PhysicalFilter into separate expressions
class SplitFilterConjunctionRule : public Rule {
public:
	SplitFilterConjunctionRule();

	unique_ptr<LogicalOperator> Apply(Rewriter &rewriter, LogicalOperator &root, vector<AbstractOperator> &bindings,
	                                  bool &fixed_point);
};

} // namespace duckdb

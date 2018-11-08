//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// optimizer/logical_rules/split_filter_conjunction.hpp
//
// Author: Hannes Mühleisen & Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/rule.hpp"

namespace duckdb {

//! Splits up AND expressions in a PhysicalFilter into separate expressions
class SplitFilterConjunctionRule : public Rule {
  public:
	SplitFilterConjunctionRule();

	std::unique_ptr<LogicalOperator>
	Apply(Rewriter &rewriter, LogicalOperator &root,
	      std::vector<AbstractOperator> &bindings, bool &fixed_point);
};

} // namespace duckdb

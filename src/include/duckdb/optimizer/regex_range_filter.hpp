//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/regex_range_filter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/rule.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class Optimizer;

class RegexRangeFilter {
public:
	RegexRangeFilter() {
	}
	//! Perform filter pushdown
	unique_ptr<LogicalOperator> Rewrite(unique_ptr<LogicalOperator> op);
};

} // namespace duckdb

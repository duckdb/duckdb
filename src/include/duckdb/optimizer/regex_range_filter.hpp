//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/regex_range_filter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/rule.hpp"

namespace duckdb {

class Optimizer;

class RegexRangeFilter {
public:
	RegexRangeFilter() {
	}
	//! Perform filter pushdown
	unique_ptr<LogicalOperator> Rewrite(unique_ptr<LogicalOperator> node);
};

} // namespace duckdb

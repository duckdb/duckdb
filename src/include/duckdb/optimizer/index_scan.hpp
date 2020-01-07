//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/index_scan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/filter_combiner.hpp"
#include "duckdb/optimizer/rule.hpp"

namespace duckdb {
class Optimizer;

class IndexScan {
public:
	//! Optimize Filters in Index Scans
	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> node);

private:
	//! Transform a Filter in an index scan
	unique_ptr<LogicalOperator> TransformFilterToIndexScan(unique_ptr<LogicalOperator> op);
};

} // namespace duckdb

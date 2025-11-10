//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/sampling_pushdown.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/common/unique_ptr.hpp"

namespace duckdb {
class LocigalOperator;
class Optimizer;

class SamplingPushdown {
public:
	//! Optimize SYSTEM SAMPLING + SCAN to SAMPLE SCAN
	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> op);
};

} // namespace duckdb

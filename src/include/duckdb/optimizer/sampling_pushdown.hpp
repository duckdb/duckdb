//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/sampling_pushdown.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/common/unique_ptr.hpp"

namespace duckdb {
class ClientContext;
class Optimizer;

class SamplingPushdown {
public:
	explicit SamplingPushdown(ClientContext &context) : context(context) {
	}
	//! Optimize SYSTEM SAMPLING + SCAN to SAMPLE SCAN
	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> op);

private:
	ClientContext &context;
};

} // namespace duckdb

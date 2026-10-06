//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/window_join_pushdown.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class WindowJoinPushdown {
public:
	explicit WindowJoinPushdown(ClientContext &client);

public:
	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> op);

private:
	ClientContext &client;
};

} // namespace duckdb

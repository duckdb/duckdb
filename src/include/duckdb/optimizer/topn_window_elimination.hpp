//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/topn_window_elimination.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/client_context.hpp"

namespace duckdb {
class LogicalOperator;
class Optimizer;

class TopNWindowElimination {
public:
	explicit TopNWindowElimination(ClientContext &context, Optimizer &optimizer);

	//! Optimize TopN window function to aggregate
	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> op);
	//! Whether we can perform the optimization on this operator
	static bool CanOptimize(LogicalOperator &op, optional_ptr<ClientContext> context = nullptr);

private:
	ClientContext &context;
	Optimizer &optimizer;
};

} // namespace duckdb

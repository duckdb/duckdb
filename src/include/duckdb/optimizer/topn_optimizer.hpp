//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/topn_optimizer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/client_context.hpp"
#include "duckdb/common/constants.hpp"

namespace duckdb {
class LogicalOperator;
class LogicalTopN;
class Optimizer;

class TopN {
public:
	explicit TopN(ClientContext &context);

	//! Optimize ORDER BY + LIMIT to TopN
	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> op);
	//! Whether we can perform the optimization on this operator
	static bool CanOptimize(LogicalOperator &op, optional_ptr<ClientContext> context = nullptr);

private:
	void PushdownDynamicFilters(LogicalTopN &op);

private:
	ClientContext &context;
};

} // namespace duckdb

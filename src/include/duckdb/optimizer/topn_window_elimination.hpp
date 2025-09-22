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

private:
	//! Whether we can perform the optimization on this operator
	static bool CanOptimize(LogicalOperator &op, optional_ptr<ClientContext> context = nullptr);

	unique_ptr<LogicalOperator> CreateAggregateOperator(vector<unique_ptr<Expression>> children, LogicalWindow &window,
	                                                    unique_ptr<Expression> limit) const;

	unique_ptr<LogicalOperator> CreateUnnestListOperator(const child_list_t<LogicalType> &input_types,
	                                                     idx_t aggregate_idx, bool include_row_number,
	                                                     unique_ptr<Expression> limit_value) const;

	unique_ptr<LogicalOperator> CreateUnnestStructOperator(const child_list_t<LogicalType> &input_types,
	                                                       idx_t unnest_list_idx, idx_t table_idx,
	                                                       bool include_row_number, idx_t row_number_idx) const;

	ClientContext &context;
	Optimizer &optimizer;
};
} // namespace duckdb

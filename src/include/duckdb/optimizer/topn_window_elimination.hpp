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

class TopNWindowElimination {
public:
	explicit TopNWindowElimination(ClientContext &context, Optimizer &optimizer);

	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> op);

private:
	static bool CanOptimize(LogicalOperator &op, optional_ptr<ClientContext> context = nullptr);

	unique_ptr<LogicalOperator> OptimizeInternal(unique_ptr<LogicalOperator> op, bool &update_table_idx,
	                                             idx_t &new_table_idx);

	unique_ptr<LogicalAggregate> CreateAggregateOperator(vector<unique_ptr<Expression>> children, LogicalWindow &window,
	                                                     unique_ptr<Expression> limit) const;

	unique_ptr<LogicalUnnest> CreateUnnestListOperator(const child_list_t<LogicalType> &input_types,
	                                                   idx_t aggregate_idx, bool include_row_number,
	                                                   unique_ptr<Expression> limit_value) const;

	unique_ptr<LogicalProjection> CreateUnnestStructOperator(const child_list_t<LogicalType> &input_types,
	                                                         idx_t unnest_list_idx, idx_t table_idx,
	                                                         bool include_row_number,
	                                                         const set<idx_t> &row_number_idxs) const;

	static void UpdateTableIdxInExpressions(LogicalProjection &projection, idx_t table_idx);
	static vector<idx_t> GeneratePaddingIdxs(const vector<ColumnBinding> &bindings);
	static void AssignChildNewTableIdx(LogicalWindow &window, idx_t table_idx);
	static bool GenerateStructPackInputExprs(const LogicalWindow &window, const vector<ColumnBinding> &bindings,
	                                         const vector<string> &column_names, const vector<LogicalType> &types,
	                                         vector<unique_ptr<Expression>> &struct_input_exprs,
	                                         set<idx_t> &row_number_idxs, bool use_new_child_idx = false,
	                                         idx_t new_child_idx = 0);

private:
	ClientContext &context;
	Optimizer &optimizer;
};
} // namespace duckdb

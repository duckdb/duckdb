//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/topn_window_elimination.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/client_context.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"

namespace duckdb {

class TopNWindowElimination {
public:
	explicit TopNWindowElimination(ClientContext &context, Optimizer &optimizer);

	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> op);

private:
	static bool CanOptimize(LogicalOperator &op, optional_ptr<ClientContext> context = nullptr);
	unique_ptr<LogicalOperator> OptimizeInternal(unique_ptr<LogicalOperator> op, ColumnBindingReplacer &replacer);

	unique_ptr<LogicalAggregate> CreateAggregateOperator(vector<unique_ptr<Expression>> children, LogicalWindow &window,
	                                                     unique_ptr<Expression> limit) const;
	unique_ptr<LogicalUnnest> CreateUnnestListOperator(const child_list_t<LogicalType> &input_types,
	                                                   idx_t aggregate_idx, bool include_row_number) const;
	unique_ptr<LogicalProjection> CreateUnnestStructOperator(const child_list_t<LogicalType> &input_types,
	                                                         idx_t unnest_list_idx, idx_t table_idx,
	                                                         bool include_row_number) const;

	static vector<unique_ptr<Expression>> GenerateStructPackExprs(const vector<ColumnBinding> &bindings,
	                                                              const LogicalWindow &window, bool &generate_row_ids);
	static void UpdateBindings(idx_t window_idx, idx_t new_table_idx, const vector<ColumnBinding> &old_bindings,
	                           vector<ColumnBinding> &new_bindings, ColumnBindingReplacer &replacer);
	static vector<ColumnBinding> TraverseProjectionBindings(const std::vector<ColumnBinding> &old_bindings,
	                                                        LogicalOperator *&op);
	static vector<LogicalType> ExtractReturnTypes(const vector<unique_ptr<Expression>> &exprs);

private:
	ClientContext &context;
	Optimizer &optimizer;
};
} // namespace duckdb

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
#include "duckdb/optimizer/remove_unused_columns.hpp"

namespace duckdb {

class TopNWindowElimination : public BaseColumnPruner {
public:
	explicit TopNWindowElimination(ClientContext &context, Optimizer &optimizer);

	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> op);

private:
	static bool CanOptimize(LogicalOperator &op, optional_ptr<ClientContext> context = nullptr);
	unique_ptr<LogicalOperator> OptimizeInternal(unique_ptr<LogicalOperator> op, ColumnBindingReplacer &replacer);

	unique_ptr<LogicalOperator> CreateAggregateOperator(LogicalWindow &window, unique_ptr<Expression> limit,
	                                                    vector<unique_ptr<Expression>> args) const;
	unique_ptr<LogicalOperator> TryCreateUnnestOperator(unique_ptr<LogicalOperator> op, bool include_row_number) const;
	unique_ptr<LogicalOperator> CreateProjectionOperator(unique_ptr<LogicalOperator> op, bool include_row_number,
	                                                     const map<idx_t, idx_t> &group_idxs) const;

	vector<unique_ptr<Expression>> GenerateAggregateArgs(const vector<ColumnBinding> &bindings,
	                                                     const LogicalWindow &window, bool &generate_row_ids,
	                                                     map<idx_t, idx_t> &group_idxs);
	static void UpdateBindings(idx_t window_idx, idx_t group_table_idx, idx_t aggregate_table_idx,
	                           const map<idx_t, idx_t> &group_idxs, const vector<ColumnBinding> &old_bindings,
	                           vector<ColumnBinding> &new_bindings, ColumnBindingReplacer &replacer);
	static vector<ColumnBinding> TraverseProjectionBindings(const std::vector<ColumnBinding> &old_bindings,
	                                                        LogicalOperator *&op);

	unique_ptr<Expression> CreateAggregateExpression(vector<unique_ptr<Expression>> aggregate_params, bool requires_arg,
	                                                 OrderType order_type) const;
	unique_ptr<Expression> CreateRowNumberGenerator(unique_ptr<Expression> aggregate_column_ref) const;
	void AddStructExtractExprs(vector<unique_ptr<Expression>> &exprs, const LogicalType &struct_type,
	                           const unique_ptr<BoundColumnRefExpression> &aggregate_column_ref) const;

private:
	ClientContext &context;
	Optimizer &optimizer;
};
} // namespace duckdb

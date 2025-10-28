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

enum class TopNPayloadType { SINGLE_COLUMN, STRUCT_PACK };

struct TopNWindowEliminationParameters {
	//! Whether the sort is ASCENDING or DESCENDING
	OrderType order_type;
	//! The number of values in the LIMIT clause
	int64_t limit;
	//! How we fetch the payload columns
	TopNPayloadType payload_type;
	//! Whether to include row numbers
	bool include_row_number;
	//! Whether the val or arg column contains null values
	bool can_be_null = false;
};

class TopNWindowElimination : public BaseColumnPruner {
public:
	explicit TopNWindowElimination(ClientContext &context, Optimizer &optimizer,
	                               optional_ptr<column_binding_map_t<unique_ptr<BaseStatistics>>> stats_p);

	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> op);

private:
	bool CanOptimize(LogicalOperator &op);
	unique_ptr<LogicalOperator> OptimizeInternal(unique_ptr<LogicalOperator> op, ColumnBindingReplacer &replacer);

	unique_ptr<LogicalOperator> CreateAggregateOperator(LogicalWindow &window, vector<unique_ptr<Expression>> args,
	                                                    const TopNWindowEliminationParameters &params) const;
	unique_ptr<LogicalOperator> TryCreateUnnestOperator(unique_ptr<LogicalOperator> op,
	                                                    const TopNWindowEliminationParameters &params) const;
	unique_ptr<LogicalOperator> CreateProjectionOperator(unique_ptr<LogicalOperator> op,
	                                                     const TopNWindowEliminationParameters &params,
	                                                     const map<idx_t, idx_t> &group_idxs) const;

	vector<unique_ptr<Expression>> GenerateAggregatePayload(const vector<ColumnBinding> &bindings,
	                                                        const LogicalWindow &window, map<idx_t, idx_t> &group_idxs);
	vector<ColumnBinding> TraverseProjectionBindings(const std::vector<ColumnBinding> &old_bindings,
	                                                 reference<LogicalOperator> &op);
	unique_ptr<Expression> CreateAggregateExpression(vector<unique_ptr<Expression>> aggregate_params, bool requires_arg,
	                                                 const TopNWindowEliminationParameters &params) const;
	unique_ptr<Expression> CreateRowNumberGenerator(unique_ptr<Expression> aggregate_column_ref) const;
	void AddStructExtractExprs(vector<unique_ptr<Expression>> &exprs, const LogicalType &struct_type,
	                           const unique_ptr<BoundColumnRefExpression> &aggregate_column_ref) const;
	static void UpdateTopmostBindings(idx_t window_idx, const unique_ptr<LogicalOperator> &op,
	                                  const map<idx_t, idx_t> &group_idxs,
	                                  const vector<ColumnBinding> &topmost_bindings,
	                                  vector<ColumnBinding> &new_bindings, ColumnBindingReplacer &replacer);
	TopNWindowEliminationParameters ExtractOptimizerParameters(const LogicalWindow &window, const LogicalFilter &filter,
	                                                           const vector<ColumnBinding> &bindings,
	                                                           vector<unique_ptr<Expression>> &aggregate_payload);

	// Semi-join reduction methods
	unique_ptr<LogicalOperator> TryPrepareLateMaterialization(const LogicalWindow &window,
	                                                          vector<unique_ptr<Expression>> &args);
	unique_ptr<LogicalOperator> ConstructLHS(LogicalGet &rhs, vector<idx_t> &projections) const;
	static unique_ptr<LogicalOperator> ConstructJoin(unique_ptr<LogicalOperator> lhs, unique_ptr<LogicalOperator> rhs,
	                                                 idx_t rhs_rowid_idx,
	                                                 const TopNWindowEliminationParameters &params);
	bool CanUseLateMaterialization(const LogicalWindow &window, vector<unique_ptr<Expression>> &args,
	                               vector<idx_t> &projections, vector<reference<LogicalOperator>> &stack);

private:
	ClientContext &context;
	Optimizer &optimizer;
	optional_ptr<column_binding_map_t<unique_ptr<BaseStatistics>>> stats;
};
} // namespace duckdb

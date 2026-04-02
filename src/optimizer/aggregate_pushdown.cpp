#include "duckdb/optimizer/aggregate_pushdown.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/optimizer/aggregate_pushdown_info.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

AggregatePushdown::AggregatePushdown(Optimizer &optimizer) : optimizer(optimizer), root(nullptr) {
}

unique_ptr<LogicalOperator> AggregatePushdown::Optimize(unique_ptr<LogicalOperator> plan) {
	root = plan.get();
	VisitOperator(plan);
	return plan;
}

void AggregatePushdown::VisitOperator(unique_ptr<LogicalOperator> &op) {
	for (auto &child : op->children) {
		if (child->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
			TryPushdown(child);
		}
		VisitOperator(child);
	}
}

bool AggregatePushdown::TryPushdown(unique_ptr<LogicalOperator> &op) {
	D_ASSERT(op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY);
	auto &aggr = op->Cast<LogicalAggregate>();

	if (!aggr.groups.empty()) {
		return false;
	}
	if (op->children.size() != 1) {
		return false;
	}

	auto *child_ptr = op->children[0].get();
	if (child_ptr->type != LogicalOperatorType::LOGICAL_GET) {
		return false;
	}
	auto &get = child_ptr->Cast<LogicalGet>();
	if (!get.function.aggregate_pushdown) {
		return false;
	}
	if (!get.children.empty()) {
		return false;
	}
	if (!get.expressions.empty()) {
		return false;
	}
	if (get.dynamic_filters) {
		// Dynamic filters (e.g., from joins) are evaluated at runtime and not accounted
		// for by the stats-based accumulation paths.
		return false;
	}
	// Expression filters (e.g., constant_or_null) are evaluated per-row during scanning.
	// The stats-based paths (AccumulateFromStats/SegmentStats) bypass row-level evaluation,
	// so they would produce wrong results if expression filters actually filter rows.
	for (const auto &filter_entry : get.table_filters) {
		auto &filter = filter_entry.Filter();
		if (filter.filter_type == TableFilterType::EXPRESSION_FILTER) {
			return false;
		}
	}

	// Keep LogicalAggregate but rewrite it to operate on partial results.
	// The scan computes partial aggregates per thread (using stats for ALWAYS_TRUE RGs, segments, pages etc.,
	// does the actual scanning for NO_PRUNING ones), outputting one row per thread.
	// The upper aggregate combines partial results: count(*) → sum, min/max → min/max.
	auto pushdown_info = make_uniq<AggregatePushdownInfo>();
	for (idx_t idx = 0; idx < aggr.expressions.size(); ++idx) {
		auto &expr = aggr.expressions[idx];
		if (expr->expression_class != ExpressionClass::BOUND_AGGREGATE) {
			return false;
		}
		auto &aggr_expr = expr->Cast<BoundAggregateExpression>();
		if (aggr_expr.aggr_type == AggregateType::DISTINCT) {
			return false;
		}
		if (aggr_expr.filter) {
			return false;
		}
		if (aggr_expr.order_bys && !aggr_expr.order_bys->orders.empty()) {
			return false;
		}

		PushedAggregateInfo info;
		info.output_idx = idx;
		
		const auto &func_name = aggr_expr.function.name;
		if (func_name != "count_star" && func_name != "count" && func_name != "min" && func_name != "max") {
			return false;
		}

		if (func_name == "count_star") {
			info.type = PushedAggregateType::COUNT_STAR;
			info.return_type = LogicalType::BIGINT;
			info.aggregate_function = make_uniq<AggregateFunction>(aggr_expr.function);
			if (aggr_expr.bind_info) {
				info.bind_data = unique_ptr<FunctionData>(aggr_expr.bind_info->Copy());
			}
		} else if (func_name == "count" || func_name == "min" || func_name == "max") {
			if (aggr_expr.children.size() != 1) {
				return false;
			}
			if (aggr_expr.children[0]->type != ExpressionType::BOUND_COLUMN_REF) {
				return false;
			}

			if (func_name == "min" || func_name == "max") {
				auto &return_type = aggr_expr.children[0]->return_type;
				if (return_type != LogicalType::VARCHAR && !return_type.IsNumeric() &&
				    !return_type.IsTemporal()) {
					// Aligned with StatisticsPropagator::GetComparator —
					// only VARCHAR (exact match), numeric, and temporal types have usable min/max stats.
					return false;
				}
			}

			auto &col_ref = aggr_expr.children[0]->Cast<BoundColumnRefExpression>();
			ColumnBinding resolved = col_ref.binding;

			if (resolved.table_index != get.table_index) {
				return false;
			}
			const auto &col_ids = get.GetColumnIds();
			if (col_ids.empty() || resolved.column_index.index >= col_ids.size()) {
				return false;
			}
			const auto &col_index = get.GetColumnIndex(resolved);
			if (!get.TryGetStorageIndex(col_index, info.col_idx)) {
				return false;
			}
			info.scan_col_position = resolved.column_index.index;
			if (func_name == "count") {
				info.type = PushedAggregateType::COUNT_COL;
				info.return_type = LogicalType::BIGINT;
				info.aggregate_function = make_uniq<AggregateFunction>(aggr_expr.function);
				if (aggr_expr.bind_info) {
					info.bind_data = unique_ptr<FunctionData>(aggr_expr.bind_info->Copy());
				}
			} else if (func_name == "min") {
				info.type = PushedAggregateType::MIN;
				info.return_type = aggr_expr.return_type;
				info.aggregate_function = make_uniq<AggregateFunction>(aggr_expr.function);
				if (aggr_expr.bind_info) {
					info.bind_data = unique_ptr<FunctionData>(aggr_expr.bind_info->Copy());
				}
			} else {
				info.type = PushedAggregateType::MAX;
				info.return_type = aggr_expr.return_type;
				info.aggregate_function = make_uniq<AggregateFunction>(aggr_expr.function);
				if (aggr_expr.bind_info) {
					info.bind_data = unique_ptr<FunctionData>(aggr_expr.bind_info->Copy());
				}
			}
		}
		pushdown_info->aggregates.push_back(std::move(info));
	}

	get.extra_info.aggregate_pushdown_info = std::move(pushdown_info);

	auto &agg_pushdown = *get.extra_info.aggregate_pushdown_info;
	const auto &old_col_ids = get.GetColumnIds();
	vector<ColumnIndex> new_col_ids;

	// Build deduplicated column list: multiple aggregates on the same column share the same one.
	for (auto &aggr : agg_pushdown.aggregates) {
		// COUNT_STAR doesn't read column values, use ROW_ID to avoid scanning a real column.
		auto col = aggr.type == PushedAggregateType::COUNT_STAR ? ColumnIndex(COLUMN_IDENTIFIER_ROW_ID)
		                                                       : old_col_ids[aggr.scan_col_position];
		bool found = false;
		for (idx_t i = 0; i < new_col_ids.size(); i++) {
			if (new_col_ids[i] == col) {
				aggr.scan_col_position = i;
				found = true;
				break;
			}
		}
		if (!found) {
			aggr.scan_col_position = new_col_ids.size();
			new_col_ids.push_back(col);
		}
	}
	// Make sure columns referenced by filters also in the scan column ids.
	for (auto &filter_entry : get.table_filters) {
		const idx_t filter_col = filter_entry.ColumnIndex();
		bool already_present = false;
		for (idx_t i = 0; i < new_col_ids.size(); i++) {
			if (new_col_ids[i].GetPrimaryIndex() == filter_col) {
				already_present = true;
				break;
			}
		}
		if (!already_present) {
			bool found = false;
			for (const auto &old_id : old_col_ids) {
				if (old_id.GetPrimaryIndex() == filter_col) {
					new_col_ids.push_back(old_id);
					found = true;
					break;
				}
			}
			if (!found) {
				new_col_ids.push_back(ColumnIndex(filter_col));
			}
		}
	}

	get.SetColumnIds(std::move(new_col_ids));
	get.projection_ids.clear();

	// Rewrite the aggregate expressions to operate on partial columns:
	//   count(*) / count(col)  →  sum(partial_count_col) + CAST projection above
	//   min(col)               →  min(partial_min_col)
	//   max(col)               →  max(partial_max_col)
	auto &catalog = Catalog::GetSystemCatalog(optimizer.context);
	FunctionBinder binder(optimizer.context);

	// Track which aggregates need a cast projection
	vector<idx_t> count_aggr_idxs;

	for (idx_t i = 0; i < aggr.expressions.size(); i++) {
		auto &aggr_expr = aggr.expressions[i]->Cast<BoundAggregateExpression>();
		const auto &func_name = aggr_expr.function.name;

		// After deduplication, projection_ids maps each aggregate's output to the
		// deduplicated column position. The upper aggregate references by output index.
		auto &pushed_agg = agg_pushdown.aggregates[i];
		ColumnBinding partial_ref(get.table_index, ProjectionIndex(i));
		const auto &partial_type = pushed_agg.return_type;

		if (func_name == "count_star" || func_name == "count") {
			auto &sum_entry =
			    catalog.GetEntry<AggregateFunctionCatalogEntry>(optimizer.context, DEFAULT_SCHEMA, "sum");
			auto sum_fun = sum_entry.functions.GetFunctionByArguments(optimizer.context, {partial_type});
			vector<unique_ptr<Expression>> args;
			args.push_back(make_uniq<BoundColumnRefExpression>(partial_type, partial_ref));
			aggr.expressions[i] =
			    binder.BindAggregateFunction(sum_fun, std::move(args), nullptr, AggregateType::NON_DISTINCT);
			// Clear statistics callback: SumPropagateStats would replace sum(BIGINT) with
			// sum_no_overflow(INTEGER) based on the original column stats, but the partial
			// aggregate output is BIGINT, not the original column type.
			auto &bound_sum = aggr.expressions[i]->Cast<BoundAggregateExpression>();
			bound_sum.function.statistics = nullptr;
			count_aggr_idxs.push_back(i);
		} else if (func_name == "min" || func_name == "max") {
			aggr_expr.children[0] = make_uniq<BoundColumnRefExpression>(partial_type, partial_ref);
		}
	}

	if (count_aggr_idxs.empty()) {
		return true;
	}

	// Insert a LogicalProjection above the aggregate to cast sum(HUGEINT) → BIGINT,
	// matching the original count(*)/count(col) return type (BIGINT).
	auto proj_index = optimizer.binder.GenerateTableIndex();
	vector<unique_ptr<Expression>> proj_exprs;
	column_binding_map_t<ColumnBinding> binding_map;

	for (idx_t i = 0; i < aggr.expressions.size(); i++) {
		ColumnBinding agg_binding(aggr.aggregate_index, ProjectionIndex(i));
		ColumnBinding new_binding(proj_index, ProjectionIndex(i));
		binding_map[agg_binding] = new_binding;

		auto agg_ref = make_uniq<BoundColumnRefExpression>(aggr.expressions[i]->return_type, agg_binding);

		auto it = std::find(count_aggr_idxs.begin(), count_aggr_idxs.end(), i);
		if (it != count_aggr_idxs.end()) {
			// COALESCE(CAST(sum_result AS BIGINT), 0): sum returns NULL for empty input,
			// but count(*) must return 0 for empty input.
			auto cast_expr =
			    BoundCastExpression::AddCastToType(optimizer.context, std::move(agg_ref), LogicalType::BIGINT);
			auto zero = make_uniq<BoundConstantExpression>(Value::BIGINT(0));
			auto coalesce = make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_COALESCE, LogicalType::BIGINT);
			coalesce->children.push_back(std::move(cast_expr));
			coalesce->children.push_back(std::move(zero));
			proj_exprs.push_back(std::move(coalesce));
		} else {
			proj_exprs.push_back(std::move(agg_ref));
		}
	}

	// Remap upper references from aggregate output to the new projection output
	ColumnBindingReplacer replacer;
	replacer.stop_operator = op.get();
	for (auto &entry : binding_map) {
		replacer.replacement_bindings.emplace_back(entry.first, entry.second);
	}
	replacer.VisitOperator(*root);

	auto proj = make_uniq<LogicalProjection>(proj_index, std::move(proj_exprs));
	proj->children.push_back(std::move(op));
	op = std::move(proj);

	return true;
}

} // namespace duckdb

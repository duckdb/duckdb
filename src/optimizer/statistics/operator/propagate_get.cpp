#include "duckdb/common/helper.hpp"
#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/function/scalar/generic_common.hpp"
#include "duckdb/function/scalar/generic_functions.hpp"

namespace duckdb {

static bool IsDirectFilterColumnRef(const Expression &expr) {
	return expr.type == ExpressionType::BOUND_REF || expr.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF;
}

static void GetColumnIndex(const unique_ptr<Expression> &expr, idx_t &index, string &alias) {
	if (expr->type == ExpressionType::BOUND_REF) {
		auto &bound_ref = expr->Cast<BoundReferenceExpression>();
		index = bound_ref.index;
		alias = bound_ref.alias;
		return;
	}
	ExpressionIterator::EnumerateChildren(*expr,
	                                      [&](unique_ptr<Expression> &child) { GetColumnIndex(child, index, alias); });
}

FilterPropagateResult StatisticsPropagator::PropagateTableFilter(ColumnBinding stats_binding, BaseStatistics &stats,
                                                                 TableFilter &filter) {
	auto &expr_filter = ExpressionFilter::GetExpressionFilter(filter, "StatisticsPropagator::PropagateTableFilter");

	// get physical storage index of the filter
	// since it is a table filter, every storage index is the same
	idx_t physical_index = DConstants::INVALID_INDEX;
	string column_alias;
	GetColumnIndex(expr_filter.expr, physical_index, column_alias);
	D_ASSERT(physical_index != DConstants::INVALID_INDEX);

	// Check statistics BEFORE HandleFilter (CheckStatistics needs pre-update stats)
	auto check_result = filter.Cast<ExpressionFilter>().CheckStatistics(stats);

	// Save original expression for stats update (HandleFilter may modify filter_expr)
	auto original_expr = expr_filter.expr->Copy();

	auto column_ref = make_uniq<BoundColumnRefExpression>(column_alias, stats.GetType(), stats_binding);
	auto filter_expr = expr_filter.ToExpression(*column_ref);
	auto propagate_result = HandleFilter(filter_expr);
	auto colref = make_uniq<BoundReferenceExpression>(column_alias, stats.GetType(), physical_index);

	// replace BoundColumnRefs with BoundRefs
	ExpressionFilter::ReplaceExpressionRecursive(filter_expr, *colref, ExpressionType::BOUND_COLUMN_REF);
	expr_filter.expr = std::move(filter_expr);

	if (propagate_result != FilterPropagateResult::NO_PRUNING_POSSIBLE) {
		// Update stats here because the caller won't call UpdateFilterStatistics for non-default cases
		UpdateExpressionFilterStatistics(stats, *original_expr);
		return propagate_result;
	}
	// For NO_PRUNING_POSSIBLE, the caller calls UpdateFilterStatistics which handles stats update
	return check_result;
}

void StatisticsPropagator::UpdateFilterStatistics(BaseStatistics &input, const TableFilter &filter) {
	// FIXME: update stats...
	auto &expr_filter = ExpressionFilter::GetExpressionFilter(filter, "StatisticsPropagator::UpdateFilterStatistics");
	UpdateExpressionFilterStatistics(input, *expr_filter.expr);
}

void StatisticsPropagator::UpdateExpressionFilterStatistics(BaseStatistics &input, const Expression &expr) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_COMPARISON: {
		auto &comp = expr.Cast<BoundComparisonExpression>();
		auto is_compare_distinct = comp.GetExpressionType() == ExpressionType::COMPARE_DISTINCT_FROM ||
		                           comp.GetExpressionType() == ExpressionType::COMPARE_NOT_DISTINCT_FROM;
		if (IsDirectFilterColumnRef(*comp.left) && comp.right->type == ExpressionType::VALUE_CONSTANT) {
			auto &constant = comp.right->Cast<BoundConstantExpression>();
			// only update if constant type matches stats type (temporal pushdown may have different types)
			if (constant.value.type().InternalType() == input.GetType().InternalType()) {
				UpdateFilterStatistics(input, comp.GetExpressionType(), constant.value);
			} else if (!is_compare_distinct) {
				input.Set(StatsInfo::CANNOT_HAVE_NULL_VALUES);
			}
		} else if (comp.left->type == ExpressionType::VALUE_CONSTANT && IsDirectFilterColumnRef(*comp.right)) {
			auto &constant = comp.left->Cast<BoundConstantExpression>();
			if (constant.value.type().InternalType() == input.GetType().InternalType()) {
				UpdateFilterStatistics(input, FlipComparisonExpression(comp.GetExpressionType()), constant.value);
			} else if (!is_compare_distinct) {
				input.Set(StatsInfo::CANNOT_HAVE_NULL_VALUES);
			}
		}
		break;
	}
	case ExpressionClass::BOUND_CONJUNCTION: {
		auto &conj = expr.Cast<BoundConjunctionExpression>();
		if (conj.GetExpressionType() == ExpressionType::CONJUNCTION_AND) {
			for (auto &child : conj.children) {
				UpdateExpressionFilterStatistics(input, *child);
			}
		}
		break;
	}
	case ExpressionClass::BOUND_OPERATOR: {
		auto &op = expr.Cast<BoundOperatorExpression>();
		if (expr.type == ExpressionType::OPERATOR_IS_NOT_NULL && !op.children.empty() &&
		    IsDirectFilterColumnRef(*op.children[0])) {
			input.Set(StatsInfo::CANNOT_HAVE_NULL_VALUES);
		}
		break;
	}
	default:
		break;
	}
}

static bool IsConstantOrNullFilter(const TableFilter &table_filter) {
	auto &expr_filter = ExpressionFilter::GetExpressionFilter(table_filter, "IsConstantOrNullFilter");
	if (expr_filter.expr->type != ExpressionType::BOUND_FUNCTION) {
		return false;
	}
	auto &func = expr_filter.expr->Cast<BoundFunctionExpression>();
	return ConstantOrNull::IsConstantOrNull(func, Value::BOOLEAN(true));
}

static bool CanReplaceConstantOrNull(const TableFilter &table_filter) {
	if (!IsConstantOrNullFilter(table_filter)) {
		throw InternalException("CanReplaceConstantOrNull() called on unexepected Table Filter");
	}
	auto &expr_filter = ExpressionFilter::GetExpressionFilter(table_filter, "CanReplaceConstantOrNull");
	auto &func = expr_filter.expr->Cast<BoundFunctionExpression>();
	if (ConstantOrNull::IsConstantOrNull(func, Value::BOOLEAN(true))) {
		for (auto child = ++func.children.begin(); child != func.children.end(); child++) {
			switch (child->get()->type) {
			case ExpressionType::BOUND_REF:
			case ExpressionType::VALUE_CONSTANT:
				continue;
			default:
				// expression type could be a function like Coalesce
				return false;
			}
		}
	}
	// all children of constant or null are bound refs to the table filter column
	return true;
}

unique_ptr<NodeStatistics> StatisticsPropagator::PropagateStatistics(LogicalGet &get,
                                                                     unique_ptr<LogicalOperator> &node_ptr) {
	if (get.function.cardinality) {
		node_stats = get.function.cardinality(context, get.bind_data.get());
	}
	if (!get.function.statistics && !get.function.statistics_extended) {
		// no column statistics to get
		return std::move(node_stats);
	}
	auto &column_ids = get.GetColumnIds();
	for (idx_t i = 0; i < column_ids.size(); i++) {
		unique_ptr<BaseStatistics> stats;
		if (get.function.statistics_extended) {
			TableFunctionGetStatisticsInput input(get.bind_data.get(), column_ids[i]);
			stats = get.function.statistics_extended(context, input);
		} else {
			stats = get.function.statistics(context, get.bind_data.get(), column_ids[i].GetPrimaryIndex());
		}

		if (stats) {
			ColumnBinding binding(get.table_index, ProjectionIndex(i));
			statistics_map.insert(make_pair(binding, std::move(stats)));
		}
	}
	// push table filters into the statistics
	vector<ProjectionIndex> filter_columns;
	filter_columns.reserve(get.table_filters.FilterCount());
	for (auto &kv : get.table_filters) {
		filter_columns.push_back(kv.GetIndex());
	}

	for (auto &table_filter_column : filter_columns) {
		// find the stats
		ColumnBinding stats_binding(get.table_index, table_filter_column);
		auto entry = statistics_map.find(stats_binding);
		if (entry == statistics_map.end()) {
			// no stats for this entry
			continue;
		}
		auto &stats = *entry->second;

		// fetch the table filter
		auto &filter = get.table_filters.GetFilterByColumnIndexMutable(table_filter_column);
		auto propagate_result = PropagateTableFilter(stats_binding, stats, filter);
		switch (propagate_result) {
		case FilterPropagateResult::FILTER_ALWAYS_TRUE:
			// filter is always true; it is useless to execute it
			// erase this condition
			get.table_filters.RemoveFilterByColumnIndex(table_filter_column);
			break;
		case FilterPropagateResult::FILTER_TRUE_OR_NULL: {
			if (IsConstantOrNullFilter(filter) && !CanReplaceConstantOrNull(filter)) {
				break;
			}
			// filter is true or null; we can replace this with a not null filter
			auto not_null =
			    make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_IS_NOT_NULL, LogicalType::BOOLEAN);
			not_null->children.push_back(make_uniq<BoundReferenceExpression>(stats.GetType(), 0));
			get.table_filters.SetFilterByColumnIndex(table_filter_column,
			                                         make_uniq<ExpressionFilter>(std::move(not_null)));
			break;
		}
		case FilterPropagateResult::FILTER_FALSE_OR_NULL:
		case FilterPropagateResult::FILTER_ALWAYS_FALSE:
			// filter is always false; this entire filter should be replaced by an empty result block
			ReplaceWithEmptyResult(node_ptr);
			return make_uniq<NodeStatistics>(0U, 0U);
		default:
			// general case: filter can be true or false, update this columns' statistics
			UpdateFilterStatistics(stats, filter);
			break;
		}
	}
	auto &generic_filters = get.table_filters.GetMutableGenericFilters();
	for (idx_t i = 0; i < generic_filters.size(); i++) {
		auto &filter_expr = generic_filters[i];
		auto propagate_result = HandleFilter(filter_expr);
		switch (propagate_result) {
		case FilterPropagateResult::FILTER_ALWAYS_TRUE:
			generic_filters.erase_at(i);
			i--;
			break;
		case FilterPropagateResult::FILTER_FALSE_OR_NULL:
		case FilterPropagateResult::FILTER_ALWAYS_FALSE:
			ReplaceWithEmptyResult(node_ptr);
			return make_uniq<NodeStatistics>(0U, 0U);
		default:
			break;
		}
	}
	return std::move(node_stats);
}

} // namespace duckdb

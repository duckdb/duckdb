#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/filter/bloom_filter.hpp"
#include "duckdb/planner/filter/dynamic_filter.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"
#include "duckdb/planner/filter/prefix_range_filter.hpp"
#include "duckdb/planner/filter/selectivity_optional_filter.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"
#include "duckdb/storage/statistics/struct_stats.hpp"
#include "duckdb/storage/statistics/string_stats.hpp"
#include "duckdb/storage/statistics/variant_stats.hpp"
#include "duckdb/function/scalar/struct_utils.hpp"

namespace duckdb {

ExpressionFilter::ExpressionFilter(unique_ptr<Expression> expr_p)
    : TableFilter(TableFilterType::EXPRESSION_FILTER), expr(std::move(expr_p)) {
}

const ExpressionFilter &ExpressionFilter::GetExpressionFilter(const TableFilter &filter, const char *context) {
	D_ASSERT(filter.filter_type == TableFilterType::EXPRESSION_FILTER);
	if (filter.filter_type != TableFilterType::EXPRESSION_FILTER) {
		throw InternalException("%s expected ExpressionFilter", context);
	}
	return filter.Cast<ExpressionFilter>();
}

ExpressionFilter &ExpressionFilter::GetExpressionFilter(TableFilter &filter, const char *context) {
	D_ASSERT(filter.filter_type == TableFilterType::EXPRESSION_FILTER);
	if (filter.filter_type != TableFilterType::EXPRESSION_FILTER) {
		throw InternalException("%s expected ExpressionFilter", context);
	}
	return filter.Cast<ExpressionFilter>();
}

unique_ptr<Expression> ExpressionFilter::CreateInExpression(unique_ptr<Expression> column, vector<Value> values) {
	auto result = make_uniq<BoundOperatorExpression>(ExpressionType::COMPARE_IN, LogicalType::BOOLEAN);
	result->GetChildrenMutable().push_back(std::move(column));
	for (auto &value : values) {
		result->GetChildrenMutable().push_back(make_uniq<BoundConstantExpression>(std::move(value)));
	}
	return std::move(result);
}

unique_ptr<Expression> ExpressionFilter::CreateNullCheckExpression(unique_ptr<Expression> column,
                                                                   ExpressionType expression_type) {
	D_ASSERT(expression_type == ExpressionType::OPERATOR_IS_NULL ||
	         expression_type == ExpressionType::OPERATOR_IS_NOT_NULL);
	auto result = make_uniq<BoundOperatorExpression>(expression_type, LogicalType::BOOLEAN);
	result->GetChildrenMutable().push_back(std::move(column));
	return std::move(result);
}

static bool IsOptionalInternalFunction(const BoundFunctionExpression &func) {
	return func.Function().GetName() == OptionalFilterScalarFun::NAME ||
	       func.Function().GetName() == SelectivityOptionalFilterScalarFun::NAME;
}

static bool IsNonSelectivityOptionalInternalFunction(const BoundFunctionExpression &func) {
	return func.Function().GetName() == OptionalFilterScalarFun::NAME;
}

static bool IsOptionalExpressionInternal(const Expression &expr, bool recurse_through_and,
                                         bool include_selectivity_optional) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
		auto &func = expr.Cast<BoundFunctionExpression>();
		return include_selectivity_optional ? IsOptionalInternalFunction(func)
		                                    : IsNonSelectivityOptionalInternalFunction(func);
	}
	if (!recurse_through_and || expr.GetExpressionClass() != ExpressionClass::BOUND_CONJUNCTION ||
	    expr.GetExpressionType() != ExpressionType::CONJUNCTION_AND) {
		return false;
	}
	auto &conj = expr.Cast<BoundConjunctionExpression>();
	if (conj.GetChildren().empty()) {
		return false;
	}
	for (auto &child : conj.GetChildren()) {
		if (!IsOptionalExpressionInternal(*child, true, include_selectivity_optional)) {
			return false;
		}
	}
	return true;
}

static bool ContainsInternalTableFilterFunction(const Expression &expr) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
		auto &func = expr.Cast<BoundFunctionExpression>();
		if (TableFilterFunctions::IsTableFilterFunction(func.Function())) {
			return true;
		}
		if (func.Function().GetName() == OptionalFilterScalarFun::NAME && func.BindInfo()) {
			auto &data = func.BindInfo()->Cast<OptionalFilterFunctionData>();
			if (data.child_filter_expr && ContainsInternalTableFilterFunction(*data.child_filter_expr)) {
				return true;
			}
		}
		if (func.Function().GetName() == SelectivityOptionalFilterScalarFun::NAME && func.BindInfo()) {
			auto &data = func.BindInfo()->Cast<SelectivityOptionalFilterFunctionData>();
			if (data.child_filter_expr && ContainsInternalTableFilterFunction(*data.child_filter_expr)) {
				return true;
			}
		}
	}
	bool found = false;
	ExpressionIterator::EnumerateChildren(expr, [&](const Expression &child) {
		if (!found) {
			found = ContainsInternalTableFilterFunction(child);
		}
	});
	return found;
}

static unique_ptr<Expression> UnwrapOptionalFiltersForConstantEvaluation(const Expression &expr) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
		auto &func = expr.Cast<BoundFunctionExpression>();
		if (func.Function().GetName() == OptionalFilterScalarFun::NAME && func.BindInfo()) {
			auto &data = func.BindInfo()->Cast<OptionalFilterFunctionData>();
			return data.child_filter_expr ? UnwrapOptionalFiltersForConstantEvaluation(*data.child_filter_expr)
			                              : make_uniq<BoundConstantExpression>(Value::BOOLEAN(true));
		}
		if (func.Function().GetName() == SelectivityOptionalFilterScalarFun::NAME && func.BindInfo()) {
			auto &data = func.BindInfo()->Cast<SelectivityOptionalFilterFunctionData>();
			return data.child_filter_expr ? UnwrapOptionalFiltersForConstantEvaluation(*data.child_filter_expr)
			                              : make_uniq<BoundConstantExpression>(Value::BOOLEAN(true));
		}
	}
	auto result = expr.Copy();
	ExpressionIterator::EnumerateChildren(
	    *result, [&](unique_ptr<Expression> &child) { child = UnwrapOptionalFiltersForConstantEvaluation(*child); });
	return result;
}

bool ExpressionFilter::EvaluateWithConstant(ClientContext &context, const Value &val) const {
	auto constant_eval_expr = UnwrapOptionalFiltersForConstantEvaluation(*expr);
	ExpressionExecutor executor(context, *constant_eval_expr);
	return EvaluateWithConstant(executor, val);
}

bool ExpressionFilter::EvaluateWithConstant(ExpressionExecutor &executor, const Value &val) const {
	DataChunk input;
	input.data.emplace_back(val, count_t(1));

	SelectionVector sel(1);

	idx_t count = executor.SelectExpression(input, sel);
	return count > 0;
}

FilterPropagateResult ExpressionFilter::CheckStatistics(const BaseStatistics &stats) const {
	if (stats.GetStatsType() == StatisticsType::GEOMETRY_STATS) {
		// Delegate to GeometryStats for geometry types
		return GeometryStats::CheckZonemap(stats, expr);
	}
	return CheckExpressionStatistics(*expr, stats);
}

FilterPropagateResult ExpressionFilter::CheckStatistics(ClientContext &context, const BaseStatistics &stats) const {
	if (stats.GetStatsType() == StatisticsType::GEOMETRY_STATS) {
		// Delegate to GeometryStats for geometry types
		return GeometryStats::CheckZonemap(stats, expr);
	}
	return CheckExpressionStatistics(&context, *expr, stats);
}

static FilterPropagateResult CheckZonemapAgainstConstants(const BaseStatistics &stats, ExpressionType comparison_type,
                                                          array_ptr<const Value> values) {
	D_ASSERT(values.size() > 0);
	switch (values[0].type().InternalType()) {
	case PhysicalType::UINT8:
	case PhysicalType::UINT16:
	case PhysicalType::UINT32:
	case PhysicalType::UINT64:
	case PhysicalType::UINT128:
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::INT64:
	case PhysicalType::INT128:
	case PhysicalType::FLOAT:
	case PhysicalType::DOUBLE:
		return NumericStats::CheckZonemap(stats, comparison_type, values);
	case PhysicalType::VARCHAR:
		if (stats.GetStatsType() == StatisticsType::STRING_STATS) {
			return StringStats::CheckZonemap(stats, comparison_type, values);
		}
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	default:
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
}

static optional_ptr<const BaseStatistics> TryGetFilterStats(optional_ptr<ClientContext> context_p,
                                                            const Expression &expr, const BaseStatistics &stats,
                                                            vector<unique_ptr<BaseStatistics>> &owned_stats) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_REF:
		return &stats;
	case ExpressionClass::BOUND_CONSTANT: {
		auto &constant = expr.Cast<BoundConstantExpression>().GetValue();
		owned_stats.push_back(BaseStatistics::FromConstant(constant).ToUnique());
		return owned_stats.back().get();
	}
	case ExpressionClass::BOUND_CAST: {
		auto &cast_expr = expr.Cast<BoundCastExpression>();
		auto child_stats = TryGetFilterStats(context_p, cast_expr.Child(), stats, owned_stats);
		if (!child_stats) {
			return nullptr;
		}
		auto cast_stats = StatisticsPropagator::TryPropagateCast(*child_stats, cast_expr.Child().GetReturnType(),
		                                                         cast_expr.GetReturnType());
		if (!cast_stats) {
			return nullptr;
		}
		if (cast_expr.IsTryCast()) {
			cast_stats->Set(StatsInfo::CAN_HAVE_NULL_VALUES);
		}
		owned_stats.push_back(std::move(cast_stats));
		return owned_stats.back().get();
	}
	case ExpressionClass::BOUND_FUNCTION: {
		auto &func = expr.Cast<BoundFunctionExpression>();

		if (!context_p) {
			// Since statistics callback need context, so this path is the fallback
			idx_t child_idx;
			if (TryGetStructExtractChildIndex(func, child_idx) && !func.GetChildren().empty()) {
				auto child_stats = TryGetFilterStats(context_p, *func.GetChildren()[0], stats, owned_stats);
				if (!child_stats || child_stats->GetType().id() != LogicalTypeId::STRUCT) {
					return nullptr;
				}
				owned_stats.push_back(StructStats::GetChildStats(*child_stats, child_idx).ToUnique());
				return owned_stats.back().get();
			}
		}

		if (!context_p || !func.Function().HasStatisticsCallback()) {
			return nullptr;
		}

		vector<BaseStatistics> child_stats;
		child_stats.reserve(func.GetChildren().size());
		for (auto &child_expr : func.GetChildren()) {
			auto child_stat = TryGetFilterStats(context_p, *child_expr, stats, owned_stats);
			if (!child_stat) {
				return nullptr;
			}
			child_stats.push_back(child_stat->Copy());
		}

		// Use copy to avoid expression rewritten
		auto expr_copy = func.Copy();
		auto &func_copy = expr_copy->Cast<BoundFunctionExpression>();
		FunctionStatisticsInput input(func_copy, func_copy.BindInfo(), child_stats, &expr_copy);
		owned_stats.push_back(func.Function().GetStatisticsCallback()(*context_p, input));
		return owned_stats.back().get();
	}
	default:
		return nullptr;
	}
}

static bool TryGetVariantComparisonStatsType(const LogicalType &typed_type, const LogicalType &constant_type,
                                             LogicalType &comparison_type) {
	if (typed_type == constant_type) {
		comparison_type = typed_type;
		return true;
	}
	if (!typed_type.IsIntegral() || !constant_type.IsIntegral()) {
		return false;
	}
	if (!LogicalType::DefaultTryGetMaxLogicalTypeUnchecked(typed_type, constant_type, comparison_type)) {
		return false;
	}
	return comparison_type.IsIntegral();
}

static optional_ptr<const BaseStatistics>
TryPrepareVariantComparisonStats(const BaseStatistics &stats, Value &constant,
                                 vector<unique_ptr<BaseStatistics>> &owned_stats) {
	if (stats.GetType().id() != LogicalTypeId::VARIANT) {
		return constant.type().id() == LogicalTypeId::VARIANT ? nullptr : &stats;
	}
	if (!VariantStats::IsShredded(stats)) {
		return nullptr;
	}
	auto &shredded_stats = VariantStats::GetShreddedStats(stats);
	if (!VariantShreddedStats::IsFullyShredded(shredded_stats)) {
		return nullptr;
	}
	auto &typed_stats = VariantStats::GetTypedStats(shredded_stats);
	auto &typed_type = typed_stats.GetType();
	if (typed_type.IsNested() || typed_type.id() == LogicalTypeId::VARIANT) {
		return nullptr;
	}

	if (constant.type().id() == LogicalTypeId::VARIANT) {
		constant = VariantValue::GetValue(constant);
	}
	if (constant.IsNull()) {
		return nullptr;
	}

	LogicalType comparison_type;
	if (!TryGetVariantComparisonStatsType(typed_type, constant.type(), comparison_type)) {
		return nullptr;
	}
	if (!constant.DefaultTryCastAs(comparison_type)) {
		return nullptr;
	}
	if (typed_type == comparison_type) {
		return &typed_stats;
	}

	auto cast_stats = StatisticsPropagator::TryPropagateCast(typed_stats, typed_type, comparison_type);
	if (!cast_stats) {
		return nullptr;
	}
	owned_stats.push_back(std::move(cast_stats));
	return owned_stats.back().get();
}

static FilterPropagateResult CheckComparisonStatistics(optional_ptr<ClientContext> context_p,
                                                       const BoundFunctionExpression &comp_expr,
                                                       const BaseStatistics &stats) {
	vector<unique_ptr<BaseStatistics>> owned_stats;
	optional_ptr<const BaseStatistics> filter_stats;
	optional_ptr<const BoundConstantExpression> constant_expr;
	auto comparison_type = comp_expr.GetExpressionType();
	auto &left = BoundComparisonExpression::Left(comp_expr);
	auto &right = BoundComparisonExpression::Right(comp_expr);
	if (right.GetExpressionType() == ExpressionType::VALUE_CONSTANT) {
		filter_stats = TryGetFilterStats(context_p, left, stats, owned_stats);
		constant_expr = &right.Cast<BoundConstantExpression>();
	} else if (left.GetExpressionType() == ExpressionType::VALUE_CONSTANT) {
		filter_stats = TryGetFilterStats(context_p, right, stats, owned_stats);
		constant_expr = &left.Cast<BoundConstantExpression>();
		comparison_type = FlipComparisonExpression(comparison_type);
	} else {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	if (!filter_stats || !constant_expr) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	auto &constant = constant_expr->GetValue();
	if (constant.IsNull()) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	auto comparison_constant = constant;
	const bool variant_comparison = filter_stats->GetType().id() == LogicalTypeId::VARIANT ||
	                                comparison_constant.type().id() == LogicalTypeId::VARIANT;
	if (variant_comparison) {
		filter_stats = TryPrepareVariantComparisonStats(*filter_stats, comparison_constant, owned_stats);
		if (!filter_stats) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		}
	}
	if (!filter_stats->CanHaveNoNull()) {
		if (variant_comparison) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		}
		return comparison_type == ExpressionType::COMPARE_DISTINCT_FROM ? FilterPropagateResult::FILTER_ALWAYS_TRUE
		                                                                : FilterPropagateResult::FILTER_ALWAYS_FALSE;
	}
	auto result =
	    CheckZonemapAgainstConstants(*filter_stats, comparison_type, array_ptr<const Value>(&comparison_constant, 1));
	if (filter_stats->CanHaveNull()) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	return result;
}

static FilterPropagateResult CheckFunctionStatistics(optional_ptr<ClientContext> context_p,
                                                     const BoundFunctionExpression &func_expr,
                                                     const BaseStatistics &stats) {
	if (BoundComparisonExpression::IsComparison(func_expr.GetExpressionType())) {
		return CheckComparisonStatistics(context_p, func_expr, stats);
	}
	if (!func_expr.Function().HasFilterPruneCallback()) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	vector<unique_ptr<BaseStatistics>> owned_stats;
	auto filter_stats = &stats;
	if (!func_expr.GetChildren().empty()) {
		auto child_stats = TryGetFilterStats(context_p, *func_expr.GetChildren()[0], stats, owned_stats);
		if (!child_stats) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		}
		filter_stats = child_stats.get();
	}
	FunctionStatisticsPruneInput input(func_expr.BindInfo().get(), *filter_stats);
	return func_expr.Function().GetFilterPruneCallback()(input);
}

static FilterPropagateResult CheckNullOperatorStatistics(optional_ptr<ClientContext> context_p,
                                                         const BoundOperatorExpression &op_expr,
                                                         const BaseStatistics &stats, ExpressionType operator_type) {
	if (op_expr.GetChildren().empty()) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	vector<unique_ptr<BaseStatistics>> owned_stats;
	auto filter_stats = TryGetFilterStats(context_p, *op_expr.GetChildren()[0], stats, owned_stats);
	if (!filter_stats) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	if (operator_type == ExpressionType::OPERATOR_IS_NULL) {
		if (!filter_stats->CanHaveNull()) {
			return FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}
		if (!filter_stats->CanHaveNoNull()) {
			return FilterPropagateResult::FILTER_ALWAYS_TRUE;
		}
	} else {
		if (!filter_stats->CanHaveNoNull()) {
			return FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}
		if (!filter_stats->CanHaveNull()) {
			return FilterPropagateResult::FILTER_ALWAYS_TRUE;
		}
	}
	return FilterPropagateResult::NO_PRUNING_POSSIBLE;
}

static FilterPropagateResult CheckInOperatorStatistics(optional_ptr<ClientContext> context_p,
                                                       const BoundOperatorExpression &op_expr,
                                                       const BaseStatistics &stats) {
	if (op_expr.GetChildren().size() <= 1) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	vector<unique_ptr<BaseStatistics>> owned_stats;
	auto filter_stats = TryGetFilterStats(context_p, *op_expr.GetChildren()[0], stats, owned_stats);
	if (!filter_stats) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	vector<Value> values;
	values.reserve(op_expr.GetChildren().size() - 1);
	for (idx_t i = 1; i < op_expr.GetChildren().size(); i++) {
		if (op_expr.GetChildren()[i]->GetExpressionType() != ExpressionType::VALUE_CONSTANT) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		}
		auto &value = op_expr.GetChildren()[i]->Cast<BoundConstantExpression>().GetValue();
		if (!value.IsNull()) {
			values.push_back(value);
		}
	}
	if (values.empty()) {
		return FilterPropagateResult::FILTER_ALWAYS_FALSE;
	}
	if (!filter_stats->CanHaveNoNull()) {
		return FilterPropagateResult::FILTER_ALWAYS_FALSE;
	}
	auto result = CheckZonemapAgainstConstants(*filter_stats, ExpressionType::COMPARE_EQUAL,
	                                           array_ptr<const Value>(values.data(), values.size()));
	if (result == FilterPropagateResult::FILTER_ALWAYS_TRUE && filter_stats->CanHaveNull()) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	return result;
}

static FilterPropagateResult CheckOperatorStatistics(optional_ptr<ClientContext> context_p,
                                                     const BoundOperatorExpression &op_expr,
                                                     const BaseStatistics &stats) {
	switch (op_expr.GetExpressionType()) {
	case ExpressionType::OPERATOR_IS_NULL:
	case ExpressionType::OPERATOR_IS_NOT_NULL:
		return CheckNullOperatorStatistics(context_p, op_expr, stats, op_expr.GetExpressionType());
	case ExpressionType::COMPARE_IN:
		return CheckInOperatorStatistics(context_p, op_expr, stats);
	default:
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
}

static FilterPropagateResult CheckConjunctionStatistics(optional_ptr<ClientContext> context_p,
                                                        const BoundConjunctionExpression &conj,
                                                        const BaseStatistics &stats) {
	switch (conj.GetExpressionType()) {
	case ExpressionType::CONJUNCTION_AND: {
		auto result = FilterPropagateResult::FILTER_ALWAYS_TRUE;
		for (auto &child : conj.GetChildren()) {
			auto prune_result = ExpressionFilter::CheckExpressionStatistics(context_p, *child, stats);
			if (prune_result == FilterPropagateResult::FILTER_ALWAYS_FALSE) {
				return FilterPropagateResult::FILTER_ALWAYS_FALSE;
			}
			if (prune_result != result) {
				result = FilterPropagateResult::NO_PRUNING_POSSIBLE;
			}
		}
		return result;
	}
	case ExpressionType::CONJUNCTION_OR:
		D_ASSERT(!conj.GetChildren().empty());
		for (auto &child : conj.GetChildren()) {
			auto prune_result = ExpressionFilter::CheckExpressionStatistics(context_p, *child, stats);
			if (prune_result == FilterPropagateResult::NO_PRUNING_POSSIBLE) {
				return FilterPropagateResult::NO_PRUNING_POSSIBLE;
			}
			if (prune_result == FilterPropagateResult::FILTER_ALWAYS_TRUE) {
				return FilterPropagateResult::FILTER_ALWAYS_TRUE;
			}
		}
		return FilterPropagateResult::FILTER_ALWAYS_FALSE;
	default:
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
}

FilterPropagateResult ExpressionFilter::CheckExpressionStatistics(const Expression &expr, const BaseStatistics &stats) {
	return CheckExpressionStatistics(nullptr, expr, stats);
}

FilterPropagateResult ExpressionFilter::CheckExpressionStatistics(optional_ptr<ClientContext> context_p,
                                                                  const Expression &expr, const BaseStatistics &stats) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_FUNCTION:
		return CheckFunctionStatistics(context_p, expr.Cast<BoundFunctionExpression>(), stats);
	case ExpressionClass::BOUND_OPERATOR:
		return CheckOperatorStatistics(context_p, expr.Cast<BoundOperatorExpression>(), stats);
	case ExpressionClass::BOUND_CONJUNCTION:
		return CheckConjunctionStatistics(context_p, expr.Cast<BoundConjunctionExpression>(), stats);
	default:
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
}

bool ExpressionFilter::ContainsInternalFunction(const Expression &expr, const string &func_name) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
		auto &func = expr.Cast<BoundFunctionExpression>();
		if (func.Function().GetName() == func_name) {
			return true;
		}
		if (func.Function().GetName() == OptionalFilterScalarFun::NAME && func.BindInfo()) {
			auto &data = func.BindInfo()->Cast<OptionalFilterFunctionData>();
			if (data.child_filter_expr && ContainsInternalFunction(*data.child_filter_expr, func_name)) {
				return true;
			}
		}
		if (func.Function().GetName() == SelectivityOptionalFilterScalarFun::NAME && func.BindInfo()) {
			auto &data = func.BindInfo()->Cast<SelectivityOptionalFilterFunctionData>();
			if (data.child_filter_expr && ContainsInternalFunction(*data.child_filter_expr, func_name)) {
				return true;
			}
		}
	}
	bool found = false;
	ExpressionIterator::EnumerateChildren(expr, [&](const Expression &child) {
		if (!found) {
			found = ContainsInternalFunction(child, func_name);
		}
	});
	return found;
}

bool ExpressionFilter::IsOptionalExpression(const Expression &expr) {
	return IsOptionalExpressionInternal(expr, true, true);
}

bool ExpressionFilter::IsRootOptionalExpression(const Expression &expr) {
	return IsOptionalExpressionInternal(expr, false, true);
}

bool ExpressionFilter::IsOptionalFilter(const TableFilter &filter) {
	auto &expr_filter = GetExpressionFilter(filter, "ExpressionFilter::IsOptionalFilter");
	return IsOptionalExpression(*expr_filter.expr);
}

bool ExpressionFilter::IsRootOptionalFilter(const TableFilter &filter) {
	auto &expr_filter = GetExpressionFilter(filter, "ExpressionFilter::IsRootOptionalFilter");
	return IsRootOptionalExpression(*expr_filter.expr);
}

bool ExpressionFilter::IsRootNonSelectivityOptionalFilter(const TableFilter &filter) {
	auto &expr_filter = GetExpressionFilter(filter, "ExpressionFilter::IsRootNonSelectivityOptionalFilter");
	return IsOptionalExpressionInternal(*expr_filter.expr, false, false);
}

static shared_ptr<DynamicFilterData> TryGetRootDynamicFilterData(const Expression &expr) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
		return nullptr;
	}
	auto &func = expr.Cast<BoundFunctionExpression>();
	if (func.Function().GetName() != DynamicFilterScalarFun::NAME || !func.BindInfo()) {
		return nullptr;
	}
	return func.BindInfo()->Cast<DynamicFilterFunctionData>().filter_data;
}

shared_ptr<DynamicFilterData> ExpressionFilter::GetRootOptionalDynamicFilterData(const TableFilter &filter) {
	auto &expr_filter = GetExpressionFilter(filter, "ExpressionFilter::GetRootOptionalDynamicFilterData");
	if (expr_filter.expr->GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
		return nullptr;
	}
	auto &func = expr_filter.expr->Cast<BoundFunctionExpression>();
	if (func.Function().GetName() == OptionalFilterScalarFun::NAME && func.BindInfo()) {
		auto &data = func.BindInfo()->Cast<OptionalFilterFunctionData>();
		return data.child_filter_expr ? TryGetRootDynamicFilterData(*data.child_filter_expr) : nullptr;
	}
	if (func.Function().GetName() == SelectivityOptionalFilterScalarFun::NAME && func.BindInfo()) {
		auto &data = func.BindInfo()->Cast<SelectivityOptionalFilterFunctionData>();
		return data.child_filter_expr ? TryGetRootDynamicFilterData(*data.child_filter_expr) : nullptr;
	}
	return nullptr;
}

unique_ptr<ExpressionFilter> ExpressionFilter::FromTableFilter(const TableFilter &filter, const LogicalType &col_type) {
	if (filter.filter_type == TableFilterType::EXPRESSION_FILTER) {
		auto &expr_filter = filter.Cast<ExpressionFilter>();
		return make_uniq<ExpressionFilter>(expr_filter.expr->Copy());
	}
	if (col_type == LogicalType::ANY) {
		throw InternalException("ExpressionFilter::FromTableFilter requires the actual column type");
	}
	storage_t col_idx = 0;
	auto col_ref = make_uniq<BoundReferenceExpression>(col_type, col_idx);
	auto expr = filter.ToExpression(*col_ref);
	return make_uniq<ExpressionFilter>(std::move(expr));
}

string ExpressionFilter::InternalFunctionToString(const BoundFunctionExpression &func_expr, const string &column_name) {
	auto &func_name = func_expr.Function().GetName();
	if (func_name == BloomFilterScalarFun::NAME) {
		auto &data = func_expr.BindInfo()->Cast<BloomFilterFunctionData>();
		return BloomFilterScalarFun::ToString(column_name, data.key_column_name);
	} else if (func_name == PrefixRangeScalarFun::NAME) {
		auto &data = func_expr.BindInfo()->Cast<PrefixRangeFunctionData>();
		return PrefixRangeScalarFun::ToString(column_name, data.key_column_name);
	} else if (func_name == DynamicFilterScalarFun::NAME) {
		const auto has_filter_data =
		    func_expr.BindInfo() && func_expr.BindInfo()->Cast<DynamicFilterFunctionData>().filter_data;
		return DynamicFilterScalarFun::ToString(column_name, has_filter_data);
	} else if (func_name == OptionalFilterScalarFun::NAME) {
		string child_filter_string;
		if (func_expr.BindInfo()) {
			auto &data = func_expr.BindInfo()->Cast<OptionalFilterFunctionData>();
			if (data.child_filter_expr) {
				child_filter_string = ExpressionToFriendlyString(*data.child_filter_expr, column_name);
			}
		}
		return OptionalFilterScalarFun::ToString(child_filter_string);
	} else if (func_name == SelectivityOptionalFilterScalarFun::NAME) {
		string child_filter_string;
		if (func_expr.BindInfo()) {
			auto &data = func_expr.BindInfo()->Cast<SelectivityOptionalFilterFunctionData>();
			if (data.child_filter_expr) {
				child_filter_string = ExpressionToFriendlyString(*data.child_filter_expr, column_name);
			}
		}
		return SelectivityOptionalFilterScalarFun::ToString(child_filter_string);
	}
	return string();
}

string ExpressionFilter::ExpressionToFriendlyString(const Expression &expression, const string &column_name) {
	if (expression.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
		auto &func_expr = expression.Cast<BoundFunctionExpression>();
		auto result = InternalFunctionToString(func_expr, column_name);
		if (!result.empty()) {
			return result;
		}
	}
	if (expression.GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION) {
		auto &conj = expression.Cast<BoundConjunctionExpression>();
		if (ContainsInternalTableFilterFunction(expression)) {
			string result = "(";
			for (idx_t i = 0; i < conj.GetChildren().size(); i++) {
				if (i > 0) {
					result += conj.GetExpressionType() == ExpressionType::CONJUNCTION_AND ? " AND " : " OR ";
				}
				result += ExpressionToFriendlyString(*conj.GetChildren()[i], column_name);
			}
			result += ")";
			return result;
		}
	}
	// Default: use standard expression ToString with column name substitution
	auto expr_copy = expression.Copy();
	auto name_expr = make_uniq<BoundReferenceExpression>(Identifier(column_name), LogicalType::INVALID, 0ULL);
	ReplaceExpressionRecursive(expr_copy, *name_expr, ExpressionType::BOUND_REF);
	return expr_copy->ToString();
}

string ExpressionFilter::ToString(const string &column_name) const {
	return ExpressionToFriendlyString(*expr, column_name);
}

string ExpressionFilter::DebugToString() const {
	return ExpressionToFriendlyString(*expr, "c1");
}

void ExpressionFilter::ReplaceExpressionRecursive(unique_ptr<Expression> &expr, const Expression &column,
                                                  ExpressionType replace_type) {
	if (expr->GetExpressionType() == replace_type) {
		expr = column.Copy();
		return;
	}
	ExpressionIterator::EnumerateChildren(
	    *expr, [&](unique_ptr<Expression> &child) { ReplaceExpressionRecursive(child, column, replace_type); });
}

unique_ptr<Expression> ExpressionFilter::ToExpression(const Expression &column) const {
	auto expr_copy = expr->Copy();
	ReplaceExpressionRecursive(expr_copy, column);
	return expr_copy;
}

bool ExpressionFilter::Equals(const ExpressionFilter &other_p) const {
	auto &other = other_p.Cast<ExpressionFilter>();
	return other.expr->Equals(*expr);
}

unique_ptr<ExpressionFilter> ExpressionFilter::Copy() const {
	return make_uniq<ExpressionFilter>(expr->Copy());
}

} // namespace duckdb

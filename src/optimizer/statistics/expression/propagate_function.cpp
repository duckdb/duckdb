#include "duckdb/optimizer/statistics_propagator.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/arg_properties.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"

namespace duckdb {

static bool TryEvaluateAtConstants(ClientContext &context, const BoundFunctionExpression &func,
                                   const vector<Value> &arg_values, Value &result) {
	vector<unique_ptr<Expression>> children;
	children.reserve(arg_values.size());
	for (auto &v : arg_values) {
		children.push_back(make_uniq<BoundConstantExpression>(v));
	}
	auto bind_info_clone = func.bind_info ? func.bind_info->Copy() : nullptr;
	BoundFunctionExpression clone(func.GetReturnType(), func.function, std::move(children), std::move(bind_info_clone),
	                              func.is_operator);
	return ExpressionExecutor::TryEvaluateScalar(context, clone, result);
}

//! Evaluate `func` at the lo/hi corner of each child's value range to derive output min/max.
//! Decreasing args are swapped so f(lo_args) and f(hi_args) bracket the output range.
static unique_ptr<BaseStatistics> TryPropagateMonotoneBounds(ClientContext &context, BoundFunctionExpression &func,
                                                             const vector<BaseStatistics> &child_stats) {
	if (!func.function.HasArgProperties() || func.children.empty()) {
		return nullptr;
	}
	if (func.function.GetStability() != FunctionStability::CONSISTENT) {
		return nullptr;
	}
	if (BaseStatistics::GetStatsType(func.GetReturnType()) != StatisticsType::NUMERIC_STATS ||
	    func.GetReturnType().InternalType() == PhysicalType::BOOL) {
		return nullptr;
	}

	vector<Value> lo_args(func.children.size());
	vector<Value> hi_args(func.children.size());
	// SPECIAL_HANDLING means the function may produce nulls on non-null inputs (e.g. try_cast).
	bool output_can_have_null = (func.function.GetNullHandling() != FunctionNullHandling::DEFAULT_NULL_HANDLING);

	for (idx_t i = 0; i < func.children.size(); i++) {
		auto &child = *func.children[i];
		auto &cs = child_stats[i];
		auto &props = func.function.GetArgProperties(i);

		if (child.IsFoldable()) {
			Value v;
			if (!ExpressionExecutor::TryEvaluateScalar(context, child, v) || v.IsNull()) {
				return nullptr;
			}
			lo_args[i] = v;
			hi_args[i] = std::move(v);
			continue;
		}

		auto m = props.monotonicity;
		if (cs.CanHaveNull()) {
			output_can_have_null = true;
		}
		if (cs.GetStatsType() != StatisticsType::NUMERIC_STATS || !NumericStats::HasMinMax(cs)) {
			return nullptr;
		}
		Value lo = NumericStats::Min(cs);
		Value hi = NumericStats::Max(cs);

		if (!IsKnownMonotonic(m)) {
			return nullptr;
		}

		if (m == Monotonicity::CONSTANT) {
			lo_args[i] = lo;
			hi_args[i] = std::move(lo);
			continue;
		}
		if (IsMonotonicIncreasing(m)) {
			lo_args[i] = std::move(lo);
			hi_args[i] = std::move(hi);
		} else {
			D_ASSERT(IsMonotonicDecreasing(m));
			lo_args[i] = std::move(hi);
			hi_args[i] = std::move(lo);
		}
	}

	Value out_lo, out_hi;
	if (!TryEvaluateAtConstants(context, func, lo_args, out_lo) ||
	    !TryEvaluateAtConstants(context, func, hi_args, out_hi)) {
		return nullptr;
	}
	// NaN/NULL at a corner means the input was NaN/NULL (column contains NaN, or year(±infinity)).
	// NaN is excluded even though DuckDB orders it above all other values: negate(NaN) = NaN, which
	// would violate NON_INCREASING if NaN were treated as a valid corner. Bail instead.
	const auto is_unusable = [](const Value &v) {
		if (v.IsNull()) {
			return true;
		}
		switch (v.type().id()) {
		case LogicalTypeId::DOUBLE:
			return Value::IsNan(v.GetValue<double>());
		case LogicalTypeId::FLOAT:
			return Value::IsNan(v.GetValue<float>());
		default:
			return false;
		}
	};
	if (is_unusable(out_lo) || is_unusable(out_hi)) {
		return nullptr;
	}
	if (out_hi < out_lo) {
		throw InternalException("Monotonic arg annotation violated for '%s': output min exceeds output max",
		                        func.function.name);
	}

	auto result = NumericStats::CreateEmpty(func.GetReturnType());
	NumericStats::SetMin(result, out_lo);
	NumericStats::SetMax(result, out_hi);

	result.Set(StatsInfo::CAN_HAVE_VALID_VALUES);
	if (output_can_have_null) {
		result.Set(StatsInfo::CAN_HAVE_NULL_VALUES);
	}
	return result.ToUnique();
}

unique_ptr<BaseStatistics> StatisticsPropagator::PropagateExpression(BoundFunctionExpression &func,
                                                                     unique_ptr<Expression> &expr_ptr) {
	vector<BaseStatistics> stats;
	stats.reserve(func.children.size());
	for (idx_t i = 0; i < func.children.size(); i++) {
		auto stat = PropagateExpression(func.children[i]);
		if (!stat) {
			stats.push_back(BaseStatistics::CreateUnknown(func.children[i]->GetReturnType()));
		} else {
			stats.push_back(stat->Copy());
		}
	}
	if (func.function.HasStatisticsCallback()) {
		FunctionStatisticsInput input(func, func.bind_info.get(), stats, &expr_ptr);
		return func.function.GetStatisticsCallback()(context, input);
	}
	return TryPropagateMonotoneBounds(context, func, stats);
}

} // namespace duckdb

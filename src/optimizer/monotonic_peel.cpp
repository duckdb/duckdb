#include "duckdb/optimizer/monotonic_peel.hpp"

#include "duckdb/common/constants.hpp"
#include "duckdb/function/arg_properties.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

bool TryPeelMonotonicLevel(const Expression &expr, MonotonicPeelStep &step, bool allow_finite_only) {
	step.inverts = false;
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_CAST) {
		auto &cast = expr.Cast<BoundCastExpression>();
		// try_cast turns failures into NULLs, which changes the NULL-set across the rewrite —
		// MIN's NULL-skipping then picks a different row in the peeled vs un-peeled query.
		if (cast.try_cast) {
			return false;
		}
		auto &props = cast.bound_cast.GetArgProperties();
		if (props.requires_finite_input && !allow_finite_only) {
			return false;
		}
		if (IsMonotonicDecreasing(props.monotonicity)) {
			step.inverts = true;
		} else if (!IsMonotonicIncreasing(props.monotonicity)) {
			return false;
		}
		step.col_arg = 0;
		return true;
	}
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
		return false;
	}
	auto &fun = expr.Cast<BoundFunctionExpression>();
	if (fun.function.GetStability() != FunctionStability::CONSISTENT) {
		return false;
	}
	if (fun.function.GetNullHandling() != FunctionNullHandling::DEFAULT_NULL_HANDLING) {
		return false;
	}
	if (!fun.function.HasArgProperties()) {
		return false;
	}
	// find the unique non-foldable arg
	idx_t non_foldable = DConstants::INVALID_INDEX;
	for (idx_t i = 0; i < fun.children.size(); i++) {
		if (fun.children[i]->IsFoldable()) {
			continue;
		}
		if (non_foldable != DConstants::INVALID_INDEX) {
			return false;
		}
		non_foldable = i;
	}
	if (non_foldable == DConstants::INVALID_INDEX) {
		return false;
	}
	auto &props = fun.function.GetArgProperties(non_foldable);
	if (props.requires_finite_input && !allow_finite_only) {
		return false;
	}
	if (IsMonotonicIncreasing(props.monotonicity)) {
		// no flip
	} else if (IsMonotonicDecreasing(props.monotonicity)) {
		step.inverts = true;
	} else {
		return false;
	}
	step.col_arg = non_foldable;
	return true;
}

const Expression &PeelColumnBearingChild(const Expression &expr, const MonotonicPeelStep &step) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_CAST) {
		return *expr.Cast<BoundCastExpression>().child;
	}
	return *expr.Cast<BoundFunctionExpression>().children[step.col_arg];
}

unique_ptr<Expression> ExtractColumnBearingChild(Expression &expr, const MonotonicPeelStep &step) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_CAST) {
		return std::move(expr.Cast<BoundCastExpression>().child);
	}
	return std::move(expr.Cast<BoundFunctionExpression>().children[step.col_arg]);
}

} // namespace duckdb

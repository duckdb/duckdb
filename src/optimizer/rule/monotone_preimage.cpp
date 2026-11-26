#include "duckdb/optimizer/rule/monotone_preimage.hpp"

#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/arg_properties.hpp"
#include "duckdb/optimizer/expression_rewriter.hpp"
#include "duckdb/optimizer/matcher/expression_matcher.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

#include <cmath>

namespace duckdb {

MonotonePreimageRule::MonotonePreimageRule(ExpressionRewriter &rewriter) : Rule(rewriter) {
	// match a comparison between a foldable constant and a unary function of some expression
	auto op = make_uniq<ComparisonExpressionMatcher>();
	op->policy = SetMatcher::Policy::UNORDERED;
	op->matchers.push_back(make_uniq<FoldableConstantMatcher>());
	auto func = make_uniq<FunctionExpressionMatcher>();
	func->matchers.push_back(make_uniq<ExpressionMatcher>());
	func->policy = SetMatcher::Policy::ORDERED;
	op->matchers.push_back(std::move(func));
	root = std::move(op);
}

//! Finite, midpoint-overflow-safe integer domain of an exact-ordered column type.
static bool GetFiniteDomain(const LogicalType &type, int64_t &lo, int64_t &hi, bool &has_infinity) {
	has_infinity = false;
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
		lo = NumericLimits<int8_t>::Minimum();
		hi = NumericLimits<int8_t>::Maximum();
		return true;
	case LogicalTypeId::SMALLINT:
		lo = NumericLimits<int16_t>::Minimum();
		hi = NumericLimits<int16_t>::Maximum();
		return true;
	case LogicalTypeId::INTEGER:
		lo = NumericLimits<int32_t>::Minimum();
		hi = NumericLimits<int32_t>::Maximum();
		return true;
	case LogicalTypeId::DATE:
		// exclude the ±infinity sentinels so bisection stays inside the finite, totally-ordered range
		lo = int64_t(date_t::ninfinity().days) + 1;
		hi = int64_t(date_t::infinity().days) - 1;
		has_infinity = true;
		return true;
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
		// every non-sentinel int64 is a valid, comparable timestamp (make_timestamp accepts all of them). Search
		// between the ±infinity sentinels so that hi+1 and lo-1 stay representable. Apply handles the finite
		// INT64_MIN below -infinity.
		lo = timestamp_t::ninfinity().value + 1;
		hi = timestamp_t::infinity().value - 1;
		has_infinity = true;
		return true;
	case LogicalTypeId::DECIMAL: {
		// the unscaled integer of a decimal is exact like the integer types
		const auto width = DecimalType::GetWidth(type);
		if (width > Decimal::MAX_WIDTH_INT64) {
			return false;
		}
		hi = NumericHelper::POWERS_OF_TEN[width] - 1;
		lo = -hi;
		return true;
	}
	default:
		// BIGINT's full int64 domain would overflow the never-true bisection sentinels, other timestamp
		// resolutions, HUGEINT and wide decimals need a wider midpoint, and floating types are only monotone up
		// to rounding (see FloatRoundingPreimage for the exceptions): all left to a later extension.
		return false;
	}
}

//! The functions rounding a FLOAT or DOUBLE to an integer. They are exact, unlike the libm functions that are
//! declared monotone, so their preimage is computed in closed form instead of by bisection.
static bool GetFloatRounding(const BoundFunctionExpression &func, const LogicalType &col_type,
                             IntegerRounding &rounding) {
	if (col_type.id() != LogicalTypeId::FLOAT && col_type.id() != LogicalTypeId::DOUBLE) {
		return false;
	}
	rounding = func.Function().GetArgProperties(0).integer_rounding;
	return rounding != IntegerRounding::UNKNOWN;
}

//! `rounding(col) >= k`, or its complement `rounding(col) < k`, as a comparison on col. NaN sorts above every number
//! and the infinities and NaN round to themselves, so this agrees with comparing the rounding for every value.
static unique_ptr<Expression> RoundingAtLeast(IntegerRounding rounding, double k, bool complement,
                                              const BoundColumnRefExpression &col, const LogicalType &col_type) {
	// floor and positive trunc reach k at k, ceil and non-positive trunc right above k - 1
	const bool from_k = rounding == IntegerRounding::FLOOR || (rounding == IntegerRounding::TRUNCATE && k > 0);
	auto type = from_k ? ExpressionType::COMPARE_GREATERTHANOREQUALTO : ExpressionType::COMPARE_GREATERTHAN;
	const double bound = from_k ? k : k - 1;
	if (complement) {
		type = NegateComparisonExpression(type);
	}
	auto value = col_type.id() == LogicalTypeId::FLOAT ? Value::FLOAT(static_cast<float>(bound)) : Value::DOUBLE(bound);
	return BoundComparisonExpression::Create(type, col.Copy(), make_uniq<BoundConstantExpression>(std::move(value)));
}

//! Preimage of `rounding(col) cmp c` on a FLOAT or DOUBLE column
static unique_ptr<Expression> FloatRoundingPreimage(ClientContext &context, IntegerRounding rounding,
                                                    ExpressionType cmp, const Value &c,
                                                    const BoundColumnRefExpression &col, const LogicalType &col_type) {
	const auto value = c.GetValue<double>();
	// the integers next to the constant must be exact, which they are not beyond 2^24 (FLOAT) or 2^53 (DOUBLE)
	const double exact_limit = col_type.id() == LogicalTypeId::FLOAT ? 16777216.0 : 9007199254740992.0;
	if (!Value::IsFinite(value) || std::fabs(value) > exact_limit - 2) {
		return nullptr;
	}
	// the rounding is an integer (or NaN or an infinity), so it is compared with the integer next to the constant
	if (cmp == ExpressionType::COMPARE_EQUAL) {
		if (std::floor(value) != value) {
			return ExpressionRewriter::ConstantOrNull(context, col.Copy(), Value::BOOLEAN(false));
		}
		auto lower = RoundingAtLeast(rounding, value, false, col, col_type);
		auto upper = RoundingAtLeast(rounding, value + 1, true, col, col_type);
		return make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(lower),
		                                             std::move(upper));
	}
	const bool at_least =
	    cmp == ExpressionType::COMPARE_GREATERTHANOREQUALTO || cmp == ExpressionType::COMPARE_GREATERTHAN;
	const bool use_ceil =
	    cmp == ExpressionType::COMPARE_GREATERTHANOREQUALTO || cmp == ExpressionType::COMPARE_LESSTHAN;
	const double k = use_ceil ? std::ceil(value) : std::floor(value) + 1;
	return RoundingAtLeast(rounding, k, !at_least, col, col_type);
}

//! Evaluate f at the probe value with internal representation `i`, returning false if the result is
//! NULL or the evaluation fails (e.g. a domain error) - in which case the rewrite must bail.
static bool EvalFunc(ClientContext &context, const BoundFunctionExpression &func, const LogicalType &col_type,
                     int64_t i, Value &out) {
	auto clone = func.Copy();
	auto &clone_func = clone->Cast<BoundFunctionExpression>();
	clone_func.GetChildrenMutable()[0] = make_uniq<BoundConstantExpression>(Value::Numeric(col_type, i));
	return ExpressionExecutor::TryEvaluateScalar(context, clone_func, out) && !out.IsNull();
}

//! Midpoint of [l, h] (l <= h) that never overflows, even on the full int64 domain where l + h or
//! h - l would.
static int64_t SafeMidpoint(int64_t l, int64_t h) {
	return l + static_cast<int64_t>((static_cast<uint64_t>(h) - static_cast<uint64_t>(l)) / 2);
}

//! First index in [lo, hi] where `pred` becomes true, assuming pred is false on a prefix and true on
//! a suffix. Returns hi+1 if never true. Sets ok=false if any probe cannot be evaluated.
template <class PRED>
static int64_t FirstTrueSuffix(PRED pred, int64_t lo, int64_t hi, bool &ok) {
	bool v;
	if (!pred(hi, v, ok) || !ok) {
		return 0;
	}
	if (!v) {
		return hi + 1;
	}
	if (!pred(lo, v, ok) || !ok) {
		return 0;
	}
	if (v) {
		return lo;
	}
	int64_t l = lo, h = hi; // pred(l) false, pred(h) true
	while (static_cast<uint64_t>(h) - static_cast<uint64_t>(l) > 1) {
		int64_t mid = SafeMidpoint(l, h);
		if (!pred(mid, v, ok) || !ok) {
			return 0;
		}
		if (v) {
			h = mid;
		} else {
			l = mid;
		}
	}
	return h;
}

//! Last index in [lo, hi] where `pred` is true, assuming pred is true on a prefix and false on a
//! suffix. Returns lo-1 if never true.
template <class PRED>
static int64_t LastTruePrefix(PRED pred, int64_t lo, int64_t hi, bool &ok) {
	bool v;
	if (!pred(lo, v, ok) || !ok) {
		return 0;
	}
	if (!v) {
		return lo - 1;
	}
	if (!pred(hi, v, ok) || !ok) {
		return 0;
	}
	if (v) {
		return hi;
	}
	int64_t l = lo, h = hi; // pred(l) true, pred(h) false
	while (static_cast<uint64_t>(h) - static_cast<uint64_t>(l) > 1) {
		int64_t mid = SafeMidpoint(l, h);
		if (!pred(mid, v, ok) || !ok) {
			return 0;
		}
		if (v) {
			l = mid;
		} else {
			h = mid;
		}
	}
	return l;
}

//! Emit `col >= bound` (lower) or `col <= bound` (upper). On types with ±infinity a bound on the edge of the
//! finite domain is written as a strict comparison against the adjacent sentinel instead (`col > -infinity`,
//! `col < infinity`), as nothing lies between the two.
static unique_ptr<Expression> MakeColBound(bool lower, const BoundColumnRefExpression &col, const LogicalType &col_type,
                                           int64_t bound, int64_t dom_edge, bool has_infinity) {
	if (has_infinity && bound == dom_edge) {
		auto sentinel = lower ? Value::NegativeInfinity(col_type) : Value::Infinity(col_type);
		return BoundComparisonExpression::Create(lower ? ExpressionType::COMPARE_GREATERTHAN
		                                               : ExpressionType::COMPARE_LESSTHAN,
		                                         col.Copy(), make_uniq<BoundConstantExpression>(std::move(sentinel)));
	}
	return BoundComparisonExpression::Create(
	    lower ? ExpressionType::COMPARE_GREATERTHANOREQUALTO : ExpressionType::COMPARE_LESSTHANOREQUALTO, col.Copy(),
	    make_uniq<BoundConstantExpression>(Value::Numeric(col_type, bound)));
}

unique_ptr<Expression> MonotonePreimageRule::Apply(LogicalOperator &op, vector<reference<Expression>> &bindings,
                                                   bool &changes_made, bool is_root) {
	auto &comparison = bindings[0].get().Cast<BoundFunctionExpression>();
	auto &constant_expr = bindings[1].get();
	auto &func = bindings[2].get().Cast<BoundFunctionExpression>();

	// the matched function must be a monotone, deterministic unary function of a bare column
	if (func.GetChildren().size() != 1 || !func.Function().HasArgProperties() ||
	    func.Function().GetStability() == FunctionStability::VOLATILE) {
		return nullptr;
	}
	auto m = func.Function().GetArgProperties(0).monotonicity;
	if (!IsKnownMonotonic(m) || m == Monotonicity::CONSTANT) {
		return nullptr;
	}
	if (func.GetChildren()[0]->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
		return nullptr;
	}
	auto &col = func.GetChildren()[0]->Cast<BoundColumnRefExpression>();
	auto &col_type = col.GetReturnType();

	int64_t dom_lo = 0, dom_hi = 0;
	bool has_infinity = false;
	IntegerRounding rounding = IntegerRounding::FLOOR;
	const bool is_float_rounding = GetFloatRounding(func, col_type, rounding);
	if (!is_float_rounding && !GetFiniteDomain(col_type, dom_lo, dom_hi, has_infinity)) {
		return nullptr;
	}
	// Infinity sentinels can make date/time functions return NULL for non-NULL inputs.
	if (has_infinity && (!is_root || op.type != LogicalOperatorType::LOGICAL_FILTER)) {
		return nullptr;
	}

	// evaluate the constant and cast it to the function's return type for the corner comparisons
	Value c;
	if (!ExpressionExecutor::TryEvaluateScalar(GetContext(), constant_expr, c) || c.IsNull()) {
		return nullptr;
	}
	if (c.type() != func.GetReturnType()) {
		auto cast = c.TryCastAs(GetContext(), func.GetReturnType());
		if (!cast) {
			return nullptr;
		}
		c = std::move(*cast);
	}

	// normalize the comparison so the function is on the left: f(col) OP c
	const bool func_is_left = RefersToSameObject(*comparison.GetChildren()[0], func);
	auto cmp = comparison.GetExpressionType();
	if (!func_is_left) {
		cmp = FlipComparisonExpression(cmp);
	}
	switch (cmp) {
	case ExpressionType::COMPARE_EQUAL:
	case ExpressionType::COMPARE_LESSTHAN:
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
	case ExpressionType::COMPARE_GREATERTHAN:
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		break;
	default:
		return nullptr;
	}

	auto &context = GetContext();
	if (is_float_rounding) {
		return FloatRoundingPreimage(context, rounding, cmp, c, col, col_type);
	}
	bool ok = true;
	auto ge = [&](int64_t i, bool &v, bool &okref) {
		Value fv;
		if (!EvalFunc(context, func, col_type, i, fv)) {
			okref = false;
			return false;
		}
		v = ValueOperations::GreaterThanEquals(fv, c);
		return true;
	};
	auto gt = [&](int64_t i, bool &v, bool &okref) {
		Value fv;
		if (!EvalFunc(context, func, col_type, i, fv)) {
			okref = false;
			return false;
		}
		v = ValueOperations::GreaterThan(fv, c);
		return true;
	};

	// only search for the boundary/boundaries this operator actually uses: >= and < need the f>=c
	// crossover, > and <= need the f>c crossover, and = needs both.
	const bool need_ge = cmp == ExpressionType::COMPARE_GREATERTHANOREQUALTO ||
	                     cmp == ExpressionType::COMPARE_LESSTHAN || cmp == ExpressionType::COMPARE_EQUAL;
	const bool need_gt = cmp == ExpressionType::COMPARE_GREATERTHAN ||
	                     cmp == ExpressionType::COMPARE_LESSTHANOREQUALTO || cmp == ExpressionType::COMPARE_EQUAL;

	// [a, b] is the inclusive internal-integer preimage interval (clamped to the finite domain)
	int64_t a, b;
	if (IsMonotonicIncreasing(m)) {
		int64_t x_ge = need_ge ? FirstTrueSuffix(ge, dom_lo, dom_hi, ok) : 0;
		int64_t x_gt = need_gt ? FirstTrueSuffix(gt, dom_lo, dom_hi, ok) : 0;
		if (!ok) {
			return nullptr;
		}
		switch (cmp) {
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			a = x_ge;
			b = dom_hi;
			break;
		case ExpressionType::COMPARE_GREATERTHAN:
			a = x_gt;
			b = dom_hi;
			break;
		case ExpressionType::COMPARE_LESSTHAN:
			a = dom_lo;
			b = x_ge - 1;
			break;
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
			a = dom_lo;
			b = x_gt - 1;
			break;
		default: // EQUAL
			a = x_ge;
			b = x_gt - 1;
			break;
		}
	} else {
		int64_t y_ge = need_ge ? LastTruePrefix(ge, dom_lo, dom_hi, ok) : 0;
		int64_t y_gt = need_gt ? LastTruePrefix(gt, dom_lo, dom_hi, ok) : 0;
		if (!ok) {
			return nullptr;
		}
		switch (cmp) {
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			a = dom_lo;
			b = y_ge;
			break;
		case ExpressionType::COMPARE_GREATERTHAN:
			a = dom_lo;
			b = y_gt;
			break;
		case ExpressionType::COMPARE_LESSTHAN:
			a = y_ge + 1;
			b = dom_hi;
			break;
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
			a = y_gt + 1;
			b = dom_hi;
			break;
		default: // EQUAL
			a = y_gt + 1;
			b = y_ge;
			break;
		}
	}

	// clamp to the finite domain
	a = MaxValue<int64_t>(a, dom_lo);
	b = MinValue<int64_t>(b, dom_hi);

	if (a > b) {
		// empty preimage. On types with ±infinity, f(±inf) is NULL (not false), so returning a plain
		// constant would mishandle those rows - bail instead.
		if (has_infinity) {
			return nullptr;
		}
		return ExpressionRewriter::ConstantOrNull(GetContext(), col.Copy(), Value::BOOLEAN(false));
	}

	// emit finite-domain bounds. On infinity types both bounds are always kept so ±infinity (whose f is
	// NULL) is excluded by construction; on infinity-free types a trivial bound is dropped.
	const bool need_lower = a > dom_lo || has_infinity;
	const bool need_upper = b < dom_hi || has_infinity;
	if (!need_lower && !need_upper) {
		return nullptr; // no narrowing
	}
	// include the finite value below -infinity when it satisfies the comparison
	bool below_sentinel_matches = false;
	if (has_infinity && a == dom_lo) {
		Value below_sentinel;
		if (!EvalFunc(context, func, col_type, dom_lo - 2, below_sentinel)) {
			return nullptr;
		}
		auto check = BoundComparisonExpression::Create(cmp, make_uniq<BoundConstantExpression>(below_sentinel),
		                                               make_uniq<BoundConstantExpression>(c));
		Value matches;
		if (!ExpressionExecutor::TryEvaluateScalar(context, *check, matches) || matches.IsNull()) {
			return nullptr;
		}
		below_sentinel_matches = BooleanValue::Get(matches);
	}
	unique_ptr<Expression> lower;
	unique_ptr<Expression> upper;
	if (need_lower && below_sentinel_matches) {
		lower =
		    BoundComparisonExpression::Create(ExpressionType::COMPARE_NOTEQUAL, col.Copy(),
		                                      make_uniq<BoundConstantExpression>(Value::NegativeInfinity(col_type)));
	} else if (need_lower) {
		lower = MakeColBound(true, col, col_type, a, dom_lo, has_infinity);
	}
	if (need_upper) {
		upper = MakeColBound(false, col, col_type, b, dom_hi, has_infinity);
	}
	if (lower && upper) {
		return make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(lower),
		                                             std::move(upper));
	}
	return lower ? std::move(lower) : std::move(upper);
}

} // namespace duckdb

#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/numeric_type.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/planner/expression_iterator.hpp"

namespace duckdb {

//! Whether an expression derives from a NUMERIC column, looking through the casts function binding inserts
static bool IsExactNumericThroughCasts(const Expression &expr) {
	if (expr.exact_numeric) {
		return true;
	}
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION && BoundCastExpression::IsCast(expr)) {
		return IsExactNumericThroughCasts(BoundCastExpression::Child(expr.Cast<BoundFunctionExpression>()));
	}
	return false;
}

bool ExpressionBinder::HasExactNumericInput(const Expression &expr) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_EXPANDED) {
		// placeholder for an expression that expands into several columns (e.g. UNNEST of a struct)
		return false;
	}
	bool found = false;
	ExpressionIterator::EnumerateChildren(expr, [&](const Expression &child) {
		if (!found && IsExactNumericThroughCasts(child)) {
			found = true;
		}
	});
	return found;
}

namespace {

//! The range of unscaled values an operand can take, as integers at `scale` fractional digits
struct ExactRange {
	hugeint_t min;
	hugeint_t max;
	uint8_t scale = 0;
};

//! Multiply the bounds of a range by a power of ten so it is expressed at a larger scale
bool AlignScale(ExactRange &range, uint8_t scale) {
	if (range.scale == scale) {
		return true;
	}
	D_ASSERT(scale > range.scale);
	idx_t shift = scale - range.scale;
	if (shift >= Hugeint::CACHED_POWERS_OF_TEN) {
		return false;
	}
	auto factor = Hugeint::POWERS_OF_TEN[shift];
	if (!Hugeint::TryMultiply(range.min, factor, range.min) || !Hugeint::TryMultiply(range.max, factor, range.max)) {
		return false;
	}
	range.scale = scale;
	return true;
}

//! The unscaled value of an exact numeric constant
bool TryGetUnscaled(const Value &value, hugeint_t &result) {
	switch (value.type().InternalType()) {
	case PhysicalType::INT8:
		result = value.GetValueUnsafe<int8_t>();
		return true;
	case PhysicalType::INT16:
		result = value.GetValueUnsafe<int16_t>();
		return true;
	case PhysicalType::INT32:
		result = value.GetValueUnsafe<int32_t>();
		return true;
	case PhysicalType::INT64:
		result = value.GetValueUnsafe<int64_t>();
		return true;
	case PhysicalType::INT128:
		result = value.GetValueUnsafe<hugeint_t>();
		return true;
	case PhysicalType::UINT8:
		result = value.GetValueUnsafe<uint8_t>();
		return true;
	case PhysicalType::UINT16:
		result = value.GetValueUnsafe<uint16_t>();
		return true;
	case PhysicalType::UINT32:
		result = value.GetValueUnsafe<uint32_t>();
		return true;
	case PhysicalType::UINT64:
		result = hugeint_t(value.GetValueUnsafe<uint64_t>());
		return true;
	default:
		// UHUGEINT and wide decimals do not fit the 128-bit range arithmetic
		return false;
	}
}

//! The range of values an operand can take: the value of a constant, otherwise everything its representation holds
bool TryGetOperandRange(const Expression &expr, ExactRange &range) {
	auto &type = expr.GetReturnType();
	if (type.id() == LogicalTypeId::SQLNULL) {
		range.min = 0;
		range.max = 0;
		range.scale = 0;
		return true;
	}
	if (!NumericType::IsExact(type)) {
		return false;
	}
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
		auto &value = expr.Cast<BoundConstantExpression>().GetValue();
		range.scale = type.id() == LogicalTypeId::DECIMAL ? DecimalType::GetScale(type) : 0;
		if (value.IsNull()) {
			range.min = 0;
			range.max = 0;
			return true;
		}
		if (!TryGetUnscaled(value, range.min)) {
			return false;
		}
		range.max = range.min;
		return true;
	}
	return NumericType::TryGetRepresentationRange(type, range.min, range.max, range.scale);
}

//! The number of integer digits and the scale of an operand, for the fallback when the range does not fit 128 bits
void GetOperandDigits(const Expression &expr, uint8_t &integer_digits, uint8_t &scale) {
	auto &type = expr.GetReturnType();
	if (type.id() == LogicalTypeId::SQLNULL) {
		integer_digits = 0;
		scale = 0;
		return;
	}
	NumericType::GetDigits(type, integer_digits, scale);
}

bool TryNegate(hugeint_t value, hugeint_t &result) {
	if (value == NumericLimits<hugeint_t>::Minimum()) {
		return false;
	}
	result = -value;
	return true;
}

} // namespace

unique_ptr<Expression> ExpressionBinder::TryBindExactNumericArithmetic(ScalarFunctionCatalogEntry &func,
                                                                       vector<unique_ptr<Expression>> &children) {
	auto &name = func.name.GetIdentifierName();
	bool is_add = name == "+";
	bool is_subtract = name == "-";
	bool is_multiply = name == "*";
	if (!is_add && !is_subtract && !is_multiply) {
		return nullptr;
	}
	if (children.empty() || children.size() > 2) {
		return nullptr;
	}
	if (children.size() == 1 && !is_subtract) {
		return nullptr;
	}
	bool any_exact = false;
	for (auto &child : children) {
		if (IsExactNumericThroughCasts(*child)) {
			any_exact = true;
		}
		auto &type = child->GetReturnType();
		if (!NumericType::IsExact(type) && type.id() != LogicalTypeId::SQLNULL) {
			// mixed with floating point or non-numeric operands: regular binding
			return nullptr;
		}
	}
	if (!any_exact) {
		return nullptr;
	}

	// the representation of the result must hold every value the operands can produce
	vector<ExactRange> ranges(children.size());
	bool have_ranges = true;
	for (idx_t i = 0; i < children.size(); i++) {
		if (!TryGetOperandRange(*children[i], ranges[i])) {
			have_ranges = false;
		}
	}

	LogicalType result_type;
	bool resolved = false;
	if (have_ranges) {
		if (children.size() == 1) {
			// unary minus
			ExactRange result;
			result.scale = ranges[0].scale;
			if (TryNegate(ranges[0].max, result.min) && TryNegate(ranges[0].min, result.max)) {
				result_type = NumericType::ForUnscaledRange(result.min, result.max, result.scale);
				resolved = true;
			}
		} else if (is_multiply) {
			idx_t scale = idx_t(ranges[0].scale) + ranges[1].scale;
			hugeint_t candidates[4];
			if (scale <= Decimal::MAX_WIDTH_DECIMAL &&
			    Hugeint::TryMultiply(ranges[0].min, ranges[1].min, candidates[0]) &&
			    Hugeint::TryMultiply(ranges[0].min, ranges[1].max, candidates[1]) &&
			    Hugeint::TryMultiply(ranges[0].max, ranges[1].min, candidates[2]) &&
			    Hugeint::TryMultiply(ranges[0].max, ranges[1].max, candidates[3])) {
				auto min = candidates[0];
				auto max = candidates[0];
				for (idx_t i = 1; i < 4; i++) {
					min = MinValue(min, candidates[i]);
					max = MaxValue(max, candidates[i]);
				}
				result_type = NumericType::ForUnscaledRange(min, max, NumericCast<uint8_t>(scale));
				resolved = true;
			}
		} else {
			auto scale = MaxValue(ranges[0].scale, ranges[1].scale);
			if (AlignScale(ranges[0], scale) && AlignScale(ranges[1], scale)) {
				hugeint_t min, max;
				bool ok;
				if (is_add) {
					min = ranges[0].min;
					max = ranges[0].max;
					ok = Hugeint::TryAddInPlace(min, ranges[1].min) && Hugeint::TryAddInPlace(max, ranges[1].max);
				} else {
					min = ranges[0].min;
					max = ranges[0].max;
					ok = Hugeint::TrySubtractInPlace(min, ranges[1].max) &&
					     Hugeint::TrySubtractInPlace(max, ranges[1].min);
				}
				if (ok) {
					result_type = NumericType::ForUnscaledRange(min, max, scale);
					resolved = true;
				}
			}
		}
	}
	if (!resolved) {
		// beyond 128 bits: size the result by digits
		uint8_t left_digits, left_scale;
		GetOperandDigits(*children[0], left_digits, left_scale);
		if (children.size() == 1) {
			result_type = NumericType::ForDigits(left_digits, left_scale);
		} else {
			uint8_t right_digits, right_scale;
			GetOperandDigits(*children[1], right_digits, right_scale);
			if (is_multiply) {
				result_type = NumericType::ForDigits(idx_t(left_digits) + right_digits,
				                                     NumericCast<uint8_t>(idx_t(left_scale) + right_scale));
			} else {
				result_type = NumericType::ForDigits(idx_t(MaxValue(left_digits, right_digits)) + 1,
				                                     MaxValue(left_scale, right_scale));
			}
		}
	}

	// cast the operands so that the regular operator produces (at least) the result representation
	vector<LogicalType> cast_types;
	if (is_multiply && result_type.id() == LogicalTypeId::DECIMAL) {
		// the decimal multiplication keeps the scales of its operands and adds their widths: widen the first operand so
		// the sum of the widths covers the result, and lift it past the fixed-size tiers when the result needs that
		auto result_width = DecimalType::GetWidth(result_type);
		uint8_t widths[2], scales[2];
		for (idx_t i = 0; i < 2; i++) {
			auto &type = children[i]->GetReturnType();
			if (type.id() == LogicalTypeId::SQLNULL) {
				widths[i] = 1;
				scales[i] = 0;
			} else if (have_ranges) {
				// the digits the operand actually needs (a constant: its value, a column: its representation)
				scales[i] = ranges[i].scale;
				widths[i] = NumericCast<uint8_t>(MaxValue<idx_t>(
				    MaxValue(NumericType::Digits(ranges[i].min), NumericType::Digits(ranges[i].max)), scales[i]));
			} else {
				NumericType::GetDigits(type, widths[i], scales[i]);
				widths[i] = NumericCast<uint8_t>(widths[i] + scales[i]);
			}
		}
		idx_t left_width = MaxValue<idx_t>(widths[0], result_width > widths[1] ? result_width - widths[1] : 0);
		if (result_width > Decimal::MAX_WIDTH_INT128 && left_width <= Decimal::MAX_WIDTH_INT128 &&
		    widths[1] <= Decimal::MAX_WIDTH_INT128) {
			left_width = Decimal::MAX_WIDTH_INT128 + 1;
		} else if (result_width > Decimal::MAX_WIDTH_INT64 && left_width <= Decimal::MAX_WIDTH_INT64 &&
		           widths[1] <= Decimal::MAX_WIDTH_INT64) {
			left_width = Decimal::MAX_WIDTH_INT64 + 1;
		}
		left_width = MaxValue<idx_t>(left_width, scales[0]);
		if (left_width > Decimal::MAX_WIDTH_DECIMAL) {
			throw OutOfRangeException("NUMERIC multiplication needs more than %d digits", Decimal::MAX_WIDTH_DECIMAL);
		}
		cast_types.push_back(LogicalType::DECIMAL(NumericCast<uint8_t>(left_width), scales[0]));
		cast_types.push_back(LogicalType::DECIMAL(MaxValue(widths[1], scales[1]), scales[1]));
	} else {
		for (idx_t i = 0; i < children.size(); i++) {
			cast_types.push_back(result_type);
		}
	}

	vector<pair<Identifier, unique_ptr<Expression>>> arguments;
	for (idx_t i = 0; i < children.size(); i++) {
		auto child = BoundCastExpression::AddCastToType(context, std::move(children[i]), cast_types[i]);
		arguments.emplace_back(Identifier(), std::move(child));
	}
	FunctionBinder function_binder(binder);
	ErrorData error;
	auto result = function_binder.BindScalarFunction(func, std::move(arguments), error, true, &binder);
	if (!result) {
		error.Throw();
	}
	result->exact_numeric = true;
	return result;
}

} // namespace duckdb

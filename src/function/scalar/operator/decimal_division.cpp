#include "duckdb/common/exception.hpp"
#include "duckdb/common/hugeint.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/types/bignum.hpp"
#include "duckdb/common/types/bignum_decimal.hpp"
#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/scalar/operator_functions.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Bind data
//===--------------------------------------------------------------------===//

struct DecimalDivBindData : public FunctionData {
	// scale_exp = s2 + result_scale - s1
	// >= 0: scale numerator up:   result_int = (a_int * 10^scale_exp) / b_int
	// <  0: scale divisor up:     result_int =  a_int / (b_int * 10^|scale_exp|)
	int64_t scale_exp;

	explicit DecimalDivBindData(int64_t scale_exp_p) : scale_exp(scale_exp_p) {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<DecimalDivBindData>(scale_exp);
	}

	bool Equals(const FunctionData &other) const override {
		return scale_exp == other.Cast<DecimalDivBindData>().scale_exp;
	}
};

//===--------------------------------------------------------------------===//
// Execute functions
//
// COMPUTE_TYPE controls intermediate arithmetic:
//   int64_t   -- when input_max_width + |scale_exp| < CACHED_POWERS_OF_TEN (product fits in int64_t)
//   hugeint_t -- otherwise
// A division with an argument or a result wider than 38 digits runs in the bignum_t tier instead (see below)
//===--------------------------------------------------------------------===//

template <class T>
static constexpr uint8_t DecimalTierWidth() {
	if constexpr (std::is_same_v<T, int16_t>) {
		return Decimal::MAX_WIDTH_INT16;
	} else if constexpr (std::is_same_v<T, int32_t>) {
		return Decimal::MAX_WIDTH_INT32;
	} else if constexpr (std::is_same_v<T, int64_t>) {
		return Decimal::MAX_WIDTH_INT64;
	} else {
		return Decimal::MAX_WIDTH_INT128;
	}
}

template <class SOURCE_TYPE, class INPUT_TYPE>
static void WidenDecimal(Vector &input, Vector &widened, idx_t count) {
	if constexpr (sizeof(SOURCE_TYPE) < sizeof(INPUT_TYPE)) {
		auto widen = [](SOURCE_TYPE value) {
			return INPUT_TYPE(value);
		};
		UnaryExecutor::Execute<SOURCE_TYPE, INPUT_TYPE>(input, widened, count, widen);
	} else {
		throw InternalException("decimal_division: argument is wider than the compute tier");
	}
}

//! Bring an argument to the physical type of the wider argument; a decimal keeps its integer value at the same scale
template <class INPUT_TYPE>
static Vector &GetDecimalInput(Vector &input, idx_t count, unique_ptr<Vector> &widened) {
	auto source_physical = input.GetType().InternalType();
	if (source_physical == GetTypeId<INPUT_TYPE>()) {
		return input;
	}
	auto scale = DecimalType::GetScale(input.GetType());
	widened = make_uniq<Vector>(LogicalType::DECIMAL(DecimalTierWidth<INPUT_TYPE>(), scale), count);
	switch (source_physical) {
	case PhysicalType::INT16:
		WidenDecimal<int16_t, INPUT_TYPE>(input, *widened, count);
		break;
	case PhysicalType::INT32:
		WidenDecimal<int32_t, INPUT_TYPE>(input, *widened, count);
		break;
	case PhysicalType::INT64:
		WidenDecimal<int64_t, INPUT_TYPE>(input, *widened, count);
		break;
	default:
		throw InternalException("decimal_division: unexpected argument physical type");
	}
	return *widened;
}

template <class INPUT_TYPE, class RESULT_TYPE, class COMPUTE_TYPE>
static void DecimalDivExecute(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &bind_data = state.expr.Cast<BoundFunctionExpression>().BindInfo()->Cast<DecimalDivBindData>();
	unique_ptr<Vector> lhs_widened;
	unique_ptr<Vector> rhs_widened;
	auto &lhs = GetDecimalInput<INPUT_TYPE>(args.data[0], args.size(), lhs_widened);
	auto &rhs = GetDecimalInput<INPUT_TYPE>(args.data[1], args.size(), rhs_widened);

	bool scale_divisor = bind_data.scale_exp < 0;
	uint32_t abs_exp = UnsafeNumericCast<uint32_t>(AbsValue(bind_data.scale_exp));
	// the fixed-size tiers are only selected when the scaled operands fit a hugeint_t
	D_ASSERT(abs_exp <= Decimal::MAX_WIDTH_INT128);

	COMPUTE_TYPE scale_factor;
	if constexpr (std::is_same_v<COMPUTE_TYPE, int64_t>) {
		scale_factor = NumericHelper::POWERS_OF_TEN[abs_exp];
	} else {
		scale_factor = Hugeint::POWERS_OF_TEN[abs_exp];
	}
	auto result_width = DecimalType::GetWidth(result.GetType());

	BinaryExecutor::Execute<INPUT_TYPE, INPUT_TYPE, RESULT_TYPE>(
	    lhs, rhs, result, args.size(), [&](INPUT_TYPE a, INPUT_TYPE b) -> RESULT_TYPE {
		    if (b == INPUT_TYPE(0)) {
			    throw InvalidInputException("decimal_division: division by zero");
		    }

		    COMPUTE_TYPE numerator, divisor;
		    if (scale_divisor) {
			    numerator = COMPUTE_TYPE(a);
			    if constexpr (std::is_same_v<COMPUTE_TYPE, int64_t>) {
				    divisor = COMPUTE_TYPE(b) * scale_factor;
			    } else {
				    if (!Hugeint::TryMultiply(hugeint_t(b), scale_factor, divisor)) {
					    throw OutOfRangeException("decimal_division: scaled divisor overflowed DECIMAL range");
				    }
			    }
		    } else {
			    numerator = COMPUTE_TYPE(a) * scale_factor;
			    divisor = COMPUTE_TYPE(b);
		    }

		    bool negative;
		    COMPUTE_TYPE abs_num, abs_div;
		    if constexpr (std::is_same_v<COMPUTE_TYPE, int64_t>) {
			    negative = (numerator < 0) != (divisor < 0);
			    abs_num = numerator < 0 ? -numerator : numerator;
			    abs_div = divisor < 0 ? -divisor : divisor;
		    } else {
			    negative = (numerator.upper < 0) != (divisor.upper < 0);
			    abs_num = numerator.upper < 0 ? -numerator : numerator;
			    abs_div = divisor.upper < 0 ? -divisor : divisor;
		    }

		    COMPUTE_TYPE q, remainder;
		    if constexpr (std::is_same_v<COMPUTE_TYPE, int64_t>) {
			    q = abs_num / abs_div;
			    remainder = abs_num % abs_div;
		    } else {
			    if (abs_div.upper == 0 && abs_div.lower <= uint64_t(NumericLimits<int64_t>::Maximum())) {
				    uint64_t r64;
				    q = Hugeint::DivModPositive(abs_num, abs_div.lower, r64);
				    remainder = hugeint_t(int64_t(r64));
			    } else {
				    q = Hugeint::DivMod(abs_num, abs_div, remainder);
			    }
		    }

		    auto distance_to_next = abs_div - remainder;
		    bool quotient_is_odd;
		    if constexpr (std::is_same_v<COMPUTE_TYPE, int64_t>) {
			    quotient_is_odd = q % 2 != 0;
		    } else {
			    quotient_is_odd = q.lower % 2 != 0;
		    }
		    if (remainder > distance_to_next || (remainder == distance_to_next && quotient_is_odd)) {
			    if constexpr (std::is_same_v<COMPUTE_TYPE, int64_t>) {
				    q++;
			    } else if (!Hugeint::TryAddInPlace(q, hugeint_t(1))) {
				    throw OutOfRangeException("decimal_division: result out of range for result type");
			    }
		    }

		    bool result_out_of_range;
		    if constexpr (std::is_same_v<COMPUTE_TYPE, int64_t>) {
			    result_out_of_range = q >= NumericHelper::POWERS_OF_TEN[result_width];
		    } else {
			    result_out_of_range = q >= Hugeint::POWERS_OF_TEN[result_width];
		    }
		    if (result_out_of_range) {
			    throw OutOfRangeException("decimal_division: result out of range for result type");
		    }

		    COMPUTE_TYPE final_val = negative ? -q : q;
		    RESULT_TYPE out;
		    if constexpr (std::is_same_v<COMPUTE_TYPE, int64_t>) {
			    if (!TryCast::Operation<int64_t, RESULT_TYPE>(final_val, out)) {
				    throw OutOfRangeException("decimal_division: result out of range for result type");
			    }
		    } else {
			    if (!Hugeint::TryCast(final_val, out)) {
				    throw OutOfRangeException("decimal_division: result out of range for result type");
			    }
		    }
		    return out;
	    });
}

template <class INPUT_TYPE, class COMPUTE_TYPE>
static scalar_function_t GetDecimalDivExecuteFunction(PhysicalType result_physical) {
	switch (result_physical) {
	case PhysicalType::INT16:
		return DecimalDivExecute<INPUT_TYPE, int16_t, COMPUTE_TYPE>;
	case PhysicalType::INT32:
		return DecimalDivExecute<INPUT_TYPE, int32_t, COMPUTE_TYPE>;
	case PhysicalType::INT64:
		return DecimalDivExecute<INPUT_TYPE, int64_t, COMPUTE_TYPE>;
	default:
		return DecimalDivExecute<INPUT_TYPE, hugeint_t, COMPUTE_TYPE>;
	}
}

//===--------------------------------------------------------------------===//
// The bignum_t tier: an argument or the result is wider than 38 digits
//
// The arguments are brought to BIGNUM blobs of their unscaled values and divided with the rounding of the fixed-size
// tiers (half to even); the quotient is a BIGNUM blob, or an integer when the result is a fixed-size decimal
//===--------------------------------------------------------------------===//

template <class SOURCE_TYPE>
static void WidenDecimalToBignum(Vector &input, Vector &widened, idx_t count) {
	auto &heap = StringVector::GetStringHeap(widened);
	UnaryExecutor::Execute<SOURCE_TYPE, string_t>(input, widened, count, [&](SOURCE_TYPE value) {
		return HugeintCastToBignum::Operation<hugeint_t>(hugeint_t(value), heap).data;
	});
}

//! Bring an argument to the bignum_t tier; a decimal keeps its unscaled value at the same scale
static Vector &GetWideDecimalInput(Vector &input, idx_t count, unique_ptr<Vector> &widened) {
	auto source_physical = input.GetType().InternalType();
	if (source_physical == PhysicalType::VARCHAR) {
		return input;
	}
	auto scale = DecimalType::GetScale(input.GetType());
	widened = make_uniq<Vector>(LogicalType::DECIMAL(Decimal::MAX_WIDTH_INT128 + 1, scale), count);
	switch (source_physical) {
	case PhysicalType::INT16:
		WidenDecimalToBignum<int16_t>(input, *widened, count);
		break;
	case PhysicalType::INT32:
		WidenDecimalToBignum<int32_t>(input, *widened, count);
		break;
	case PhysicalType::INT64:
		WidenDecimalToBignum<int64_t>(input, *widened, count);
		break;
	case PhysicalType::INT128:
		WidenDecimalToBignum<hugeint_t>(input, *widened, count);
		break;
	default:
		throw InternalException("decimal_division: unexpected argument physical type");
	}
	return *widened;
}

//! RESULT_TYPE is string_t for a result wider than 38 digits, the integer of the result tier otherwise
template <class RESULT_TYPE>
static void WideDecimalDivExecute(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &bind_data = state.expr.Cast<BoundFunctionExpression>().BindInfo()->Cast<DecimalDivBindData>();
	unique_ptr<Vector> lhs_widened;
	unique_ptr<Vector> rhs_widened;
	auto &lhs = GetWideDecimalInput(args.data[0], args.size(), lhs_widened);
	auto &rhs = GetWideDecimalInput(args.data[1], args.size(), rhs_widened);

	const auto scale_exp = bind_data.scale_exp;
	const auto result_width = DecimalType::GetWidth(result.GetType());
	DecimalHelper<bignum_t>::RescaleState scratch(state.GetContext().TryGetClientContext());

	BinaryExecutor::Execute<bignum_t, bignum_t, RESULT_TYPE>(
	    lhs, rhs, result, args.size(), [&](bignum_t a, bignum_t b) -> RESULT_TYPE {
		    // the quotient of the previous row has been consumed
		    scratch.Reset();
		    bignum_t quotient;
		    if (!DecimalHelper<bignum_t>::TryDivide(scratch, a, b, scale_exp, DecimalRounding::HALF_TO_EVEN,
		                                            quotient)) {
			    throw InvalidInputException("decimal_division: division by zero");
		    }
		    // rounding can carry into a digit the result type does not have
		    if (!DecimalHelper<bignum_t>::FitsWidth(quotient, result_width)) {
			    throw OutOfRangeException("decimal_division: result out of range for result type");
		    }
		    if constexpr (std::is_same_v<RESULT_TYPE, string_t>) {
			    return StringVector::AddStringOrBlob(result, quotient.data);
		    } else {
			    hugeint_t value;
			    RESULT_TYPE out;
			    if (!DecimalHelper<bignum_t>::TryToHugeint(quotient, value) || !Hugeint::TryCast(value, out)) {
				    throw OutOfRangeException("decimal_division: result out of range for result type");
			    }
			    return out;
		    }
	    });
}

static scalar_function_t GetWideDecimalDivExecuteFunction(PhysicalType result_physical) {
	switch (result_physical) {
	case PhysicalType::INT16:
		return WideDecimalDivExecute<int16_t>;
	case PhysicalType::INT32:
		return WideDecimalDivExecute<int32_t>;
	case PhysicalType::INT64:
		return WideDecimalDivExecute<int64_t>;
	case PhysicalType::INT128:
		return WideDecimalDivExecute<hugeint_t>;
	default:
		return WideDecimalDivExecute<string_t>;
	}
}

//===--------------------------------------------------------------------===//
// Bind function
//===--------------------------------------------------------------------===//

static void SelectDecimalDivFunction(BoundScalarFunction &bound_function);

// Result width and scale for e1 / e2 (SQL Server semantics):
//   result_scale = max(6, s1 + p2 + 1)
//   result_width = p1 - s1 + s2 + result_scale
// Both are bounded by the widest DECIMAL the session binds (the max_decimal_width setting, or an argument that is
// wider), like the results of the other decimal operators: a session bound to the fixed-size decimals
// (max_decimal_width = 38) raises the errors of the fixed-size division.
static void DecimalDivisionResolveTypes(ResolveScalarFunctionTypesInput &input) {
	auto &bound_function = input.GetBoundFunction();
	auto &lhs_type = input.GetArgumentType(0);
	auto &rhs_type = input.GetArgumentType(1);

	uint32_t p1, s1, p2, s2;
	if (!lhs_type.GetDecimalProperties(p1, s1) || !rhs_type.GetDecimalProperties(p2, s2)) {
		throw InvalidInputException("decimal_division: both arguments must be DECIMAL");
	}
	// the widest DECIMAL the division constructs: the limit of the session, or a DECIMAL argument that is wider
	const auto decimal_width_limit = input.GetDecimalWidthLimit();
	uint32_t max_decimal_width = 0;
	if (lhs_type.id() == LogicalTypeId::DECIMAL) {
		max_decimal_width = MaxValue<uint32_t>(max_decimal_width, p1);
	}
	if (rhs_type.id() == LogicalTypeId::DECIMAL) {
		max_decimal_width = MaxValue<uint32_t>(max_decimal_width, p2);
	}
	const idx_t width_limit = decimal_width_limit.ResultWidth(max_decimal_width);
	// an integer argument is an implicit DECIMAL(p, 0), bound on its canonical DECIMAL type within that width: in a
	// session bound to the fixed-size decimals an integer type with more digits is a DECIMAL(38,0)
	if (lhs_type.id() != LogicalTypeId::DECIMAL) {
		p1 = NumericCast<uint32_t>(MinValue<idx_t>(p1, width_limit));
	}
	if (rhs_type.id() != LogicalTypeId::DECIMAL) {
		p2 = NumericCast<uint32_t>(MinValue<idx_t>(p2, width_limit));
	}
	auto lhs_decimal_type = lhs_type.id() == LogicalTypeId::DECIMAL ? lhs_type : LogicalType::DECIMAL(p1, s1);
	auto rhs_decimal_type = rhs_type.id() == LogicalTypeId::DECIMAL ? rhs_type : LogicalType::DECIMAL(p2, s2);

	idx_t result_scale;
	if (input.GetArgumentCount() == 3) {
		auto scale_val = input.GetNonNullConstant(2);
		auto scale_i = scale_val.GetValue<int32_t>();
		if (scale_i < 0 || idx_t(scale_i) > width_limit) {
			throw InvalidInputException("decimal_division: scale must be between 0 and %llu, got %d", width_limit,
			                            scale_i);
		}
		result_scale = UnsafeNumericCast<idx_t>(scale_i);
	} else {
		result_scale = MaxValue<idx_t>(6, idx_t(s1) + idx_t(p2) + 1);
	}
	// result_width uses signed arithmetic to catch underflow before casting.
	auto result_width_i = int64_t(p1) - int64_t(s1) + int64_t(s2) + int64_t(result_scale);
	if (result_width_i < 1) {
		throw InvalidInputException("decimal_division: scale %llu produces a zero-width result for these input types",
		                            result_scale);
	}
	idx_t result_width = UnsafeNumericCast<idx_t>(result_width_i);

	if (result_scale > width_limit) {
		throw OutOfRangeException(
		    "Needed scale %llu to accurately represent the division result, but this is out of range of the "
		    "DECIMAL type. Max scale is %llu; could not perform an accurate division. Either add a cast to DOUBLE, "
		    "or add an explicit cast to a decimal with a lower scale.",
		    result_scale, width_limit);
	}
	if (result_width > width_limit && !Decimal::IsWide(width_limit)) {
		// the session is bound to the fixed-size decimals: the division does not narrow its result
		throw OutOfRangeException(
		    "Needed width %llu to accurately represent the division result, but this is out of range of the "
		    "DECIMAL type. Max width is %llu; could not perform an accurate division. Either add a cast to DOUBLE, "
		    "or add an explicit cast to a decimal with a lower scale.",
		    result_width, width_limit);
	}

	bound_function.SetReturnType(
	    LogicalType::DECIMAL(decimal_width_limit.CheckedWidth(result_width, max_decimal_width, "Division"),
	                         NumericCast<uint32_t>(result_scale)));
	// keep the argument types: a narrower argument is widened at execution, so binding again yields the same types
	bound_function.GetArguments()[0] = lhs_decimal_type;
	bound_function.GetArguments()[1] = rhs_decimal_type;
	SelectDecimalDivFunction(bound_function);
}

//! Pick the execute function from the resolved argument and return types
static void SelectDecimalDivFunction(BoundScalarFunction &bound_function) {
	auto &arguments = bound_function.GetArguments();
	const auto result_physical = bound_function.GetReturnType().InternalType();
	if (result_physical == PhysicalType::VARCHAR || arguments[0].InternalType() == PhysicalType::VARCHAR ||
	    arguments[1].InternalType() == PhysicalType::VARCHAR) {
		// an argument or the result is wider than 38 digits
		bound_function.SetFunctionCallback(GetWideDecimalDivExecuteFunction(result_physical));
		return;
	}
	const auto wider = MaxValue<PhysicalType>(arguments[0].InternalType(), arguments[1].InternalType());
	const int64_t scale_exp = int64_t(DecimalType::GetScale(arguments[1])) +
	                          int64_t(DecimalType::GetScale(bound_function.GetReturnType())) -
	                          int64_t(DecimalType::GetScale(arguments[0]));
	const int64_t abs_scale_exp = scale_exp < 0 ? -scale_exp : scale_exp;

	switch (wider) {
	case PhysicalType::INT16:
		if (Decimal::MAX_WIDTH_INT16 + abs_scale_exp < NumericHelper::CACHED_POWERS_OF_TEN) {
			bound_function.SetFunctionCallback(GetDecimalDivExecuteFunction<int16_t, int64_t>(result_physical));
		} else {
			bound_function.SetFunctionCallback(GetDecimalDivExecuteFunction<int16_t, hugeint_t>(result_physical));
		}
		break;
	case PhysicalType::INT32:
		if (Decimal::MAX_WIDTH_INT32 + abs_scale_exp < NumericHelper::CACHED_POWERS_OF_TEN) {
			bound_function.SetFunctionCallback(GetDecimalDivExecuteFunction<int32_t, int64_t>(result_physical));
		} else {
			bound_function.SetFunctionCallback(GetDecimalDivExecuteFunction<int32_t, hugeint_t>(result_physical));
		}
		break;
	case PhysicalType::INT64:
		if (Decimal::MAX_WIDTH_INT64 + abs_scale_exp < NumericHelper::CACHED_POWERS_OF_TEN) {
			bound_function.SetFunctionCallback(GetDecimalDivExecuteFunction<int64_t, int64_t>(result_physical));
		} else {
			bound_function.SetFunctionCallback(GetDecimalDivExecuteFunction<int64_t, hugeint_t>(result_physical));
		}
		break;
	default:
		bound_function.SetFunctionCallback(GetDecimalDivExecuteFunction<hugeint_t, hugeint_t>(result_physical));
		break;
	}
}

static unique_ptr<FunctionData> DecimalDivisionBind(BindScalarFunctionInput &input) {
	auto &bound_function = input.GetBoundFunction();
	auto lhs_scale = DecimalType::GetScale(bound_function.GetArguments()[0]);
	auto rhs_scale = DecimalType::GetScale(bound_function.GetArguments()[1]);
	auto result_scale = DecimalType::GetScale(bound_function.GetReturnType());
	return make_uniq<DecimalDivBindData>(int64_t(rhs_scale) + int64_t(result_scale) - int64_t(lhs_scale));
}

//===--------------------------------------------------------------------===//
// Registration
//===--------------------------------------------------------------------===//

ScalarFunctionSet DecimalDivisionFun::GetFunctions() {
	auto decimal_type = LogicalType(LogicalTypeId::DECIMAL);
	ScalarFunctionSet set("decimal_division");

	ScalarFunction two_arg({}, decimal_type, DecimalDivExecute<hugeint_t, hugeint_t, hugeint_t>, DecimalDivisionBind);
	two_arg.GetSignature().AddParameter("x", decimal_type).AddParameter("y", decimal_type);
	two_arg.SetResolveTypesCallback(DecimalDivisionResolveTypes);
	two_arg.SetFallible();
	set.AddFunction(two_arg);

	ScalarFunction three_arg({}, decimal_type, DecimalDivExecute<hugeint_t, hugeint_t, hugeint_t>, DecimalDivisionBind);
	three_arg.GetSignature()
	    .AddParameter("x", decimal_type)
	    .AddParameter("y", decimal_type)
	    .AddParameter("scale", LogicalType::INTEGER);
	three_arg.SetResolveTypesCallback(DecimalDivisionResolveTypes);
	three_arg.SetFallible();
	set.AddFunction(three_arg);

	return set;
}

} // namespace duckdb

#include "duckdb/common/exception.hpp"
#include "duckdb/common/hugeint.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
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
	// Exponent for the scale factor applied to the numerator before dividing:
	//   result_int = (a_int * 10^scale_exp) / b_int
	// where scale_exp = s2 + result_scale - s1
	uint8_t scale_exp;

	explicit DecimalDivBindData(uint8_t scale_exp_p) : scale_exp(scale_exp_p) {
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
//   int64_t   -- INT16 inputs only; scale_exp <= 13, max numerator ~10^17, fits in int64_t
//   hugeint_t -- INT32/64/128 inputs; numerator can reach 10^37+
//===--------------------------------------------------------------------===//

template <class INPUT_TYPE, class RESULT_TYPE, class COMPUTE_TYPE>
static void DecimalDivExecute(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &bind_data = state.expr.Cast<BoundFunctionExpression>().BindInfo()->Cast<DecimalDivBindData>();

	COMPUTE_TYPE scale_factor;
	if constexpr (std::is_same_v<COMPUTE_TYPE, int64_t>) {
		scale_factor = NumericHelper::POWERS_OF_TEN[bind_data.scale_exp];
	} else {
		scale_factor = Hugeint::POWERS_OF_TEN[bind_data.scale_exp];
	}

	BinaryExecutor::Execute<INPUT_TYPE, INPUT_TYPE, RESULT_TYPE>(
	    args.data[0], args.data[1], result, args.size(), [&](INPUT_TYPE a, INPUT_TYPE b) -> RESULT_TYPE {
		    if (b == INPUT_TYPE(0)) {
			    throw InvalidInputException("decimal_division: division by zero");
		    }

		    COMPUTE_TYPE numerator = COMPUTE_TYPE(a) * scale_factor;
		    COMPUTE_TYPE divisor = COMPUTE_TYPE(b);

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

		    COMPUTE_TYPE q;
		    if constexpr (std::is_same_v<COMPUTE_TYPE, int64_t>) {
			    q = abs_num / abs_div;
		    } else {
			    if (abs_div.upper == 0) {
				    uint64_t r64;
				    q = Hugeint::DivModPositive(abs_num, abs_div.lower, r64);
			    } else {
				    hugeint_t r;
				    q = Hugeint::DivMod(abs_num, abs_div, r);
			    }
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
// Bind function
//===--------------------------------------------------------------------===//

// Result width and scale for e1 / e2 (SQL Server semantics):
//   result_scale = max(6, s1 + p2 + 1)
//   result_width = p1 - s1 + s2 + result_scale
// Both are capped at Decimal::MAX_WIDTH_DECIMAL (38).
static unique_ptr<FunctionData> DecimalDivisionBind(BindScalarFunctionInput &input) {
	auto &bound_function = input.GetBoundFunction();
	auto &arguments = input.GetArguments();

	uint8_t p1, s1, p2, s2;
	if (!arguments[0]->GetReturnType().GetDecimalProperties(p1, s1) ||
	    !arguments[1]->GetReturnType().GetDecimalProperties(p2, s2)) {
		throw InvalidInputException("decimal_division: both arguments must be DECIMAL");
	}

	uint8_t min_scale = 6;
	if (arguments.size() == 3) {
		if (!arguments[2]->IsFoldable()) {
			throw NotImplementedException("decimal_division: min_scale argument must be a constant integer");
		}
		Value min_scale_val = ExpressionExecutor::EvaluateScalar(input.GetClientContext(), *arguments[2]);
		if (min_scale_val.IsNull()) {
			throw InvalidInputException("decimal_division: min_scale argument must not be NULL");
		}
		int32_t min_scale_i = min_scale_val.GetValue<int32_t>();
		if (min_scale_i < 0 || min_scale_i > Decimal::MAX_WIDTH_DECIMAL) {
			throw InvalidInputException("decimal_division: min_scale must be between 0 and %d, got %d",
			                            Decimal::MAX_WIDTH_DECIMAL, min_scale_i);
		}
		min_scale = UnsafeNumericCast<uint8_t>(min_scale_i);
	}

	uint8_t result_scale = MaxValue<uint8_t>(min_scale, s1 + p2 + 1);
	uint8_t result_width = p1 - s1 + s2 + result_scale;

	if (result_scale > Decimal::MAX_WIDTH_DECIMAL) {
		throw OutOfRangeException(
		    "Needed scale %d to accurately represent the division result, but this is out of range of the "
		    "DECIMAL type. Max scale is %d; could not perform an accurate division. Either add a cast to DOUBLE, "
		    "or add an explicit cast to a decimal with a lower scale.",
		    result_scale, Decimal::MAX_WIDTH_DECIMAL);
	}
	if (result_width > Decimal::MAX_WIDTH_DECIMAL) {
		throw OutOfRangeException(
		    "Needed width %d to accurately represent the division result, but this is out of range of the "
		    "DECIMAL type. Max width is %d; could not perform an accurate division. Either add a cast to DOUBLE, "
		    "or add an explicit cast to a decimal with a lower scale.",
		    result_width, Decimal::MAX_WIDTH_DECIMAL);
	}

	PhysicalType result_physical;
	if (result_width <= Decimal::MAX_WIDTH_INT16) {
		result_physical = PhysicalType::INT16;
	} else if (result_width <= Decimal::MAX_WIDTH_INT32) {
		result_physical = PhysicalType::INT32;
	} else if (result_width <= Decimal::MAX_WIDTH_INT64) {
		result_physical = PhysicalType::INT64;
	} else {
		result_physical = PhysicalType::INT128;
	}

	bound_function.SetReturnType(LogicalType::DECIMAL(result_width, result_scale));

	auto lhs_physical = arguments[0]->GetReturnType().InternalType();
	auto rhs_physical = arguments[1]->GetReturnType().InternalType();
	auto wider = MaxValue<PhysicalType>(lhs_physical, rhs_physical);

	switch (wider) {
	case PhysicalType::INT16:
		// scale_exp <= 13, max numerator ~10^17 — int64_t intermediates are safe
		bound_function.GetArguments()[0] = LogicalType::DECIMAL(4, s1);
		bound_function.GetArguments()[1] = LogicalType::DECIMAL(4, s2);
		bound_function.SetFunctionCallback(GetDecimalDivExecuteFunction<int16_t, int64_t>(result_physical));
		break;
	case PhysicalType::INT32:
		bound_function.GetArguments()[0] = LogicalType::DECIMAL(9, s1);
		bound_function.GetArguments()[1] = LogicalType::DECIMAL(9, s2);
		bound_function.SetFunctionCallback(GetDecimalDivExecuteFunction<int32_t, hugeint_t>(result_physical));
		break;
	case PhysicalType::INT64:
		bound_function.GetArguments()[0] = LogicalType::DECIMAL(18, s1);
		bound_function.GetArguments()[1] = LogicalType::DECIMAL(18, s2);
		bound_function.SetFunctionCallback(GetDecimalDivExecuteFunction<int64_t, hugeint_t>(result_physical));
		break;
	case PhysicalType::INT128:
		bound_function.GetArguments()[0] = LogicalType::DECIMAL(38, s1);
		bound_function.GetArguments()[1] = LogicalType::DECIMAL(38, s2);
		bound_function.SetFunctionCallback(GetDecimalDivExecuteFunction<hugeint_t, hugeint_t>(result_physical));
		break;
	default:
		throw InternalException("decimal_division: unexpected physical type");
	}

	uint8_t scale_exp = s2 + result_scale - s1;
	return make_uniq<DecimalDivBindData>(scale_exp);
}

//===--------------------------------------------------------------------===//
// Registration
//===--------------------------------------------------------------------===//

ScalarFunctionSet DecimalDivisionFun::GetFunctions() {
	auto decimal_type = LogicalType(LogicalTypeId::DECIMAL);
	ScalarFunctionSet set("decimal_division");

	ScalarFunction two_arg({decimal_type, decimal_type}, decimal_type,
	                       DecimalDivExecute<hugeint_t, hugeint_t, hugeint_t>, DecimalDivisionBind);
	two_arg.SetFallible();
	set.AddFunction(two_arg);

	ScalarFunction three_arg({decimal_type, decimal_type, LogicalType::INTEGER}, decimal_type,
	                         DecimalDivExecute<hugeint_t, hugeint_t, hugeint_t>, DecimalDivisionBind);
	three_arg.SetFallible();
	set.AddFunction(three_arg);

	return set;
}

} // namespace duckdb

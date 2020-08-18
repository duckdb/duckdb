#include "duckdb/function/scalar/math_functions.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/scalar/trigonometric_functions.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/numeric_helper.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include <cmath>
#include <errno.h>

using namespace std;

namespace duckdb {

template <class TR, class OP> static scalar_function_t GetScalarIntegerUnaryFunctionFixedReturn(LogicalType type) {
	scalar_function_t function;
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
		function = &ScalarFunction::UnaryFunction<int8_t, TR, OP>;
		break;
	case LogicalTypeId::SMALLINT:
		function = &ScalarFunction::UnaryFunction<int16_t, TR, OP>;
		break;
	case LogicalTypeId::INTEGER:
		function = &ScalarFunction::UnaryFunction<int32_t, TR, OP>;
		break;
	case LogicalTypeId::BIGINT:
		function = &ScalarFunction::UnaryFunction<int64_t, TR, OP>;
		break;
	case LogicalTypeId::HUGEINT:
		function = &ScalarFunction::UnaryFunction<hugeint_t, TR, OP>;
		break;
	default:
		throw NotImplementedException("Unimplemented type for GetScalarIntegerUnaryFunctionFixedReturn");
	}
	return function;
}

struct UnaryDoubleWrapper {
	template <class FUNC, class OP, class INPUT_TYPE, class RESULT_TYPE>
	static inline RESULT_TYPE Operation(FUNC fun, INPUT_TYPE input, nullmask_t &nullmask, idx_t idx) {
		RESULT_TYPE result = OP::template Operation<INPUT_TYPE, RESULT_TYPE>(input);
		if (std::isnan(result) || std::isinf(result) || errno != 0) {
			errno = 0;
			nullmask[idx] = true;
			return 0;
		}
		return result;
	}
};

template <class T, class OP>
static void UnaryDoubleFunctionWrapper(DataChunk &input, ExpressionState &state, Vector &result) {
	assert(input.column_count() >= 1);
	errno = 0;
	UnaryExecutor::Execute<T, T, OP, true, UnaryDoubleWrapper>(input.data[0], result, input.size());
}

struct BinaryDoubleWrapper {
	template <class FUNC, class OP, class TA, class TB, class TR>
	static inline TR Operation(FUNC fun, TA left, TB right, nullmask_t &nullmask, idx_t idx) {
		TR result = OP::template Operation<TA, TB, TR>(left, right);
		if (std::isnan(result) || std::isinf(result) || errno != 0) {
			errno = 0;
			nullmask[idx] = true;
			return 0;
		}
		return result;
	}
};

template <class T, class OP>
static void BinaryDoubleFunctionWrapper(DataChunk &input, ExpressionState &state, Vector &result) {
	assert(input.column_count() >= 2);
	errno = 0;
	BinaryExecutor::Execute<T, T, T, OP, true, BinaryDoubleWrapper>(input.data[0], input.data[1], result, input.size());
}

//===--------------------------------------------------------------------===//
// abs
//===--------------------------------------------------------------------===//
struct AbsOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		return input < 0 ? -input : input;
	}
};

template<class OP>
unique_ptr<FunctionData> decimal_unary_op_bind(ClientContext &context, ScalarFunction &bound_function, vector<unique_ptr<Expression>> &arguments) {
	auto decimal_type = arguments[0]->return_type;
	if (decimal_type.width() <= Decimal::MAX_WIDTH_INT16) {
		bound_function.function = ScalarFunction::GetScalarUnaryFunction<OP>(LogicalTypeId::SMALLINT);
	} else if (decimal_type.width() <= Decimal::MAX_WIDTH_INT32) {
		bound_function.function = ScalarFunction::GetScalarUnaryFunction<OP>(LogicalTypeId::INTEGER);
	} else if (decimal_type.width() <= Decimal::MAX_WIDTH_INT64) {
		bound_function.function = ScalarFunction::GetScalarUnaryFunction<OP>(LogicalTypeId::BIGINT);
	} else {
		assert(decimal_type.width() <= Decimal::MAX_WIDTH_INT128);
		bound_function.function = ScalarFunction::GetScalarUnaryFunction<OP>(LogicalTypeId::HUGEINT);
	}
	bound_function.return_type = decimal_type;
	return nullptr;
}

void AbsFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet abs("abs");
	for (auto &type : LogicalType::NUMERIC) {
		if (type.id() == LogicalTypeId::DECIMAL) {
			abs.AddFunction(ScalarFunction({type}, type, nullptr, false, decimal_unary_op_bind<AbsOperator>));
		} else {
			abs.AddFunction(ScalarFunction({type}, type, ScalarFunction::GetScalarUnaryFunction<AbsOperator>(type)));
		}
	}
	set.AddFunction(abs);
}

//===--------------------------------------------------------------------===//
// bit_count
//===--------------------------------------------------------------------===//
struct BitCntOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		using TU = typename make_unsigned<TA>::type;
		TR count = 0;
		for (auto value = TU(input); value > 0; value >>= 1) {
			count += TR(value & 1);
		}
		return count;
	}
};

void BitCountFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet functions("bit_count");
	functions.AddFunction(ScalarFunction({LogicalType::TINYINT}, LogicalType::TINYINT,
	                                     ScalarFunction::UnaryFunction<int8_t, int8_t, BitCntOperator>));
	functions.AddFunction(ScalarFunction({LogicalType::SMALLINT}, LogicalType::TINYINT,
	                                     ScalarFunction::UnaryFunction<int16_t, int8_t, BitCntOperator>));
	functions.AddFunction(ScalarFunction({LogicalType::INTEGER}, LogicalType::TINYINT,
	                                     ScalarFunction::UnaryFunction<int32_t, int8_t, BitCntOperator>));
	functions.AddFunction(ScalarFunction({LogicalType::BIGINT}, LogicalType::TINYINT,
	                                     ScalarFunction::UnaryFunction<int64_t, int8_t, BitCntOperator>));
	set.AddFunction(functions);
}

//===--------------------------------------------------------------------===//
// sign
//===--------------------------------------------------------------------===//
struct SignOperator {
	template <class TA, class TR> static inline TR Operation(TA left) {
		if (left == TA(0))
			return 0;
		else if (left > TA(0))
			return 1;
		else
			return -1;
	}
};

void SignFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet sign("sign");
	for (auto &type : LogicalType::NUMERIC) {
		if (type.id() == LogicalTypeId::DECIMAL) {
			continue;
		} else {
			sign.AddFunction(ScalarFunction({type}, LogicalType::TINYINT,
											ScalarFunction::GetScalarUnaryFunctionFixedReturn<int8_t, SignOperator>(type)));
		}
	}
	set.AddFunction(sign);
}

//===--------------------------------------------------------------------===//
// ceil
//===--------------------------------------------------------------------===//
struct CeilOperator {
	template <class TA, class TR> static inline TR Operation(TA left) {
		return ceil(left);
	}
};

template<class T, class POWERS_OF_TEN, class OP>
static void generic_round_function_decimal(DataChunk &input, ExpressionState &state, Vector &result) {
	auto &func_expr = (BoundFunctionExpression &)state.expr;
	OP::template Operation<T, POWERS_OF_TEN>(input, func_expr.children[0]->return_type.scale(), result);
};

template<class OP>
unique_ptr<FunctionData> bind_generic_round_function_decimal(ClientContext &context, ScalarFunction &bound_function, vector<unique_ptr<Expression>> &arguments) {
	// ceil essentially removes the scale
	auto decimal_type = arguments[0]->return_type;
	if (decimal_type.scale() == 0) {
		// round with scale 0 is a nop
		bound_function.function = ScalarFunction::NopFunction;
	} else if (decimal_type.width() <= Decimal::MAX_WIDTH_INT16) {
		bound_function.function = generic_round_function_decimal<int16_t, NumericHelper, OP>;
	} else if (decimal_type.width() <= Decimal::MAX_WIDTH_INT32) {
		bound_function.function = generic_round_function_decimal<int32_t, NumericHelper, OP>;
	} else if (decimal_type.width() <= Decimal::MAX_WIDTH_INT64) {
		bound_function.function = generic_round_function_decimal<int64_t, NumericHelper, OP>;
	} else {
		assert(decimal_type.width() <= Decimal::MAX_WIDTH_INT128);
		bound_function.function = generic_round_function_decimal<hugeint_t, Hugeint, OP>;
	}
	bound_function.return_type = LogicalType(LogicalTypeId::DECIMAL, decimal_type.width(), 0);
	return nullptr;
}

struct CeilDecimalOperator {
	template<class T, class POWERS_OF_TEN>
	static void Operation(DataChunk &input, uint8_t scale, Vector &result) {
		T power_of_ten = POWERS_OF_TEN::PowersOfTen[scale];
		UnaryExecutor::Execute<T, T>(input.data[0], result, input.size(), [&](T input) {
			if (input < 0) {
				// below 0 we floor the number (e.g. -10.5 -> -10)
				return input / power_of_ten;
			} else {
				// above 0 we ceil the number
				return ((input - 1) / power_of_ten) + 1;
			}
		});
	}
};

void CeilFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet ceil("ceil");
	for (auto &type : LogicalType::NUMERIC) {
		scalar_function_t func = nullptr;
		bind_scalar_function_t bind_func = nullptr;
		if (type.IsIntegral()) {
			// no ceil for integral numbers
			continue;
		}
		switch(type.id()) {
		case LogicalTypeId::FLOAT:
			func = ScalarFunction::UnaryFunction<float, float, CeilOperator>;
			break;
		case LogicalTypeId::DOUBLE:
			func = ScalarFunction::UnaryFunction<double, double, CeilOperator>;
			break;
		case LogicalTypeId::DECIMAL:
			bind_func = bind_generic_round_function_decimal<CeilDecimalOperator>;
			break;
		default:
			throw NotImplementedException("Unimplemented numeric type for function \"ceil\"");
		}
		ceil.AddFunction(ScalarFunction({type}, type, func, false, bind_func));
	}

	set.AddFunction(ceil);
	ceil.name = "ceiling";
	set.AddFunction(ceil);
}

//===--------------------------------------------------------------------===//
// floor
//===--------------------------------------------------------------------===//
struct FloorOperator {
	template <class TA, class TR> static inline TR Operation(TA left) {
		return floor(left);
	}
};

struct FloorDecimalOperator {
	template<class T, class POWERS_OF_TEN>
	static void Operation(DataChunk &input, uint8_t scale, Vector &result) {
		T power_of_ten = POWERS_OF_TEN::PowersOfTen[scale];
		UnaryExecutor::Execute<T, T>(input.data[0], result, input.size(), [&](T input) {
			if (input < 0) {
				// below 0 we ceil the number (e.g. -10.5 -> -11)
				return ((input + 1) / power_of_ten) - 1;
			} else {
				// above 0 we floor the number
				return input / power_of_ten;
			}
		});
	}
};

void FloorFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet floor("floor");
	for (auto &type : LogicalType::NUMERIC) {
		scalar_function_t func = nullptr;
		bind_scalar_function_t bind_func = nullptr;
		if (type.IsIntegral()) {
			// no floor for integral numbers
			continue;
		}
		switch(type.id()) {
		case LogicalTypeId::FLOAT:
			func = ScalarFunction::UnaryFunction<float, float, FloorOperator>;
			break;
		case LogicalTypeId::DOUBLE:
			func = ScalarFunction::UnaryFunction<double, double, FloorOperator>;
			break;
		case LogicalTypeId::DECIMAL:
			bind_func = bind_generic_round_function_decimal<FloorDecimalOperator>;
			break;
		default:
			throw NotImplementedException("Unimplemented numeric type for function \"floor\"");
		}
		floor.AddFunction(ScalarFunction({type}, type, func, false, bind_func));
	}
	set.AddFunction(floor);
}

//===--------------------------------------------------------------------===//
// round
//===--------------------------------------------------------------------===//
struct RoundOperatorPrecision {
	template <class TA, class TB, class TR> static inline TR Operation(TA input, TB precision) {
		if (precision < 0) {
			precision = 0;
		}
		double modifier = pow(10, precision);
		double rounded_value = (round(input * modifier)) / modifier;
		if (std::isinf(rounded_value) || std::isnan(rounded_value)) {
			return input;
		}
		return rounded_value;
	}
};

struct RoundOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		double rounded_value = round(input);
		if (std::isinf(rounded_value) || std::isnan(rounded_value)) {
			return input;
		}
		return rounded_value;
	}
};

struct RoundDecimalOperator {
	template<class T, class POWERS_OF_TEN>
	static void Operation(DataChunk &input, uint8_t scale, Vector &result) {
		T power_of_ten = POWERS_OF_TEN::PowersOfTen[scale];
		T addition = power_of_ten / 2;
		// regular round rounds towards the nearest number
		// in case of a tie we round away from zero
		// i.e. -10.5 -> -11, 10.5 -> 11
		// we implement this by adding (positive) or subtracting (negative) 0.5
		// and then flooring the number
		// e.g. 10.5 + 0.5 = 11, floor(11) = 11
		//      10.4 + 0.5 = 10.9, floor(10.9) = 10
		UnaryExecutor::Execute<T, T>(input.data[0], result, input.size(), [&](T input) {
			if (input < 0) {
				input -= addition;
			} else {
				input += addition;
			}
			return input / power_of_ten;
		});
	}
};

struct RoundPrecisionFunctionData : public FunctionData {
	RoundPrecisionFunctionData(int32_t target_scale) : target_scale(target_scale) {}

	int32_t target_scale;

	unique_ptr<FunctionData> Copy() override {
		return make_unique<RoundPrecisionFunctionData>(target_scale);
	}
};

template<class T, class POWERS_OF_TEN>
static void decimal_round_negative_precision_function(DataChunk &input, ExpressionState &state, Vector &result) {
	auto &func_expr = (BoundFunctionExpression &)state.expr;
	auto &info = (RoundPrecisionFunctionData&) *func_expr.bind_info;
	auto source_scale = func_expr.children[0]->return_type.scale();
	if (-info.target_scale >= func_expr.children[0]->return_type.width()) {
		// scale too big for width
		result.vector_type = VectorType::CONSTANT_VECTOR;
		result.SetValue(0, Value::INTEGER(0));
		return;
	}
	T divide_power_of_ten = POWERS_OF_TEN::PowersOfTen[-info.target_scale + source_scale];
	T multiply_power_of_ten = POWERS_OF_TEN::PowersOfTen[-info.target_scale];
	T addition = divide_power_of_ten / 2;
	UnaryExecutor::Execute<T, T>(input.data[0], result, input.size(), [&](T input) {
		if (input < 0) {
			input -= addition;
		} else {
			input += addition;
		}
		return input / divide_power_of_ten * multiply_power_of_ten;
	});
}

template<class T, class POWERS_OF_TEN>
static void decimal_round_positive_precision_function(DataChunk &input, ExpressionState &state, Vector &result) {
	auto &func_expr = (BoundFunctionExpression &)state.expr;
	auto &info = (RoundPrecisionFunctionData&) *func_expr.bind_info;
	auto source_scale = func_expr.children[0]->return_type.scale();
	T power_of_ten = POWERS_OF_TEN::PowersOfTen[source_scale - info.target_scale];
	T addition = power_of_ten / 2;
	UnaryExecutor::Execute<T, T>(input.data[0], result, input.size(), [&](T input) {
		if (input < 0) {
			input -= addition;
		} else {
			input += addition;
		}
		return input / power_of_ten;
	});
};

unique_ptr<FunctionData> bind_decimal_round_precision(ClientContext &context, ScalarFunction &bound_function, vector<unique_ptr<Expression>> &arguments) {
	auto decimal_type = arguments[0]->return_type;
	if (!arguments[1]->IsFoldable()) {
		throw NotImplementedException("ROUND(DECIMAL, INTEGER) with non-constant precision is not supported");
	}
	Value val = ExpressionExecutor::EvaluateScalar(*arguments[1]).CastAs(LogicalType::INTEGER);
	if (val.is_null) {
		throw NotImplementedException("ROUND(DECIMAL, INTEGER) expected a numeric precision field");
	}
	// our new precision becomes the round value
	// e.g. ROUND(DECIMAL(18,3), 1) -> DECIMAL(18,1)
	// but ONLY if the round value is positive
	// if it is negative the scale becomes zero
	// i.e. ROUND(DECIMAL(18,3), -1) -> DECIMAL(18,0)
	int32_t round_value = val.value_.integer;
	uint8_t target_scale;
	if (round_value < 0) {
		target_scale = 0;
		if (decimal_type.width() <= Decimal::MAX_WIDTH_INT16) {
			bound_function.function = decimal_round_negative_precision_function<int16_t, NumericHelper>;
		} else if (decimal_type.width() <= Decimal::MAX_WIDTH_INT32) {
			bound_function.function = decimal_round_negative_precision_function<int32_t, NumericHelper>;
		} else if (decimal_type.width() <= Decimal::MAX_WIDTH_INT64) {
			bound_function.function = decimal_round_negative_precision_function<int64_t, NumericHelper>;
		} else {
			assert(decimal_type.width() <= Decimal::MAX_WIDTH_INT128);
			bound_function.function = decimal_round_negative_precision_function<hugeint_t, Hugeint>;
		}
	} else {
		if (round_value >= (int32_t) decimal_type.scale()) {
			// if round_value is bigger than or equal to scale we do nothing
			bound_function.function = ScalarFunction::NopFunction;
			target_scale = decimal_type.scale();
		} else {
			target_scale = round_value;
			if (decimal_type.width() <= Decimal::MAX_WIDTH_INT16) {
				bound_function.function = decimal_round_positive_precision_function<int16_t, NumericHelper>;
			} else if (decimal_type.width() <= Decimal::MAX_WIDTH_INT32) {
				bound_function.function = decimal_round_positive_precision_function<int32_t, NumericHelper>;
			} else if (decimal_type.width() <= Decimal::MAX_WIDTH_INT64) {
				bound_function.function = decimal_round_positive_precision_function<int64_t, NumericHelper>;
			} else {
				assert(decimal_type.width() <= Decimal::MAX_WIDTH_INT128);
				bound_function.function = decimal_round_positive_precision_function<hugeint_t, Hugeint>;
			}
		}
	}
	bound_function.return_type = LogicalType(LogicalTypeId::DECIMAL, decimal_type.width(), target_scale);
	return make_unique<RoundPrecisionFunctionData>(round_value);
}

void RoundFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet round("round");
	for (auto &type : LogicalType::NUMERIC) {
		scalar_function_t round_prec_func = nullptr;
		scalar_function_t round_func = nullptr;
		bind_scalar_function_t bind_func = nullptr;
		bind_scalar_function_t bind_prec_func = nullptr;
		if (type.IsIntegral()) {
			// no round for integral numbers
			continue;
		}
		switch(type.id()) {
		case LogicalTypeId::FLOAT:
			round_func = ScalarFunction::UnaryFunction<float, float, RoundOperator>;
			round_prec_func = ScalarFunction::BinaryFunction<float, int32_t, float, RoundOperatorPrecision>;
			break;
		case LogicalTypeId::DOUBLE:
			round_func = ScalarFunction::UnaryFunction<double, double, RoundOperator>;
			round_prec_func = ScalarFunction::BinaryFunction<double, int32_t, double, RoundOperatorPrecision>;
			break;
		case LogicalTypeId::DECIMAL:
			bind_func = bind_generic_round_function_decimal<RoundDecimalOperator>;
			bind_prec_func = bind_decimal_round_precision;
			break;
		default:
			throw NotImplementedException("Unimplemented numeric type for function \"floor\"");
		}
		round.AddFunction(ScalarFunction({type}, type, round_func, false, bind_func));
		round.AddFunction(ScalarFunction({type, LogicalType::INTEGER}, type, round_prec_func, false, bind_prec_func));
	}
	set.AddFunction(round);
}

//===--------------------------------------------------------------------===//
// exp
//===--------------------------------------------------------------------===//
struct ExpOperator {
	template <class TA, class TR> static inline TR Operation(TA left) {
		return exp(left);
	}
};

void ExpFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("exp", {LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                               UnaryDoubleFunctionWrapper<double, ExpOperator>));
}

//===--------------------------------------------------------------------===//
// pow
//===--------------------------------------------------------------------===//
struct PowOperator {
	template <class TA, class TB, class TR> static inline TR Operation(TA base, TB exponent) {
		return pow(base, exponent);
	}
};

void PowFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunction power_function("pow", {LogicalType::DOUBLE, LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                              BinaryDoubleFunctionWrapper<double, PowOperator>);
	set.AddFunction(power_function);
	power_function.name = "power";
	set.AddFunction(power_function);
	power_function.name = "**";
	set.AddFunction(power_function);
}

//===--------------------------------------------------------------------===//
// sqrt
//===--------------------------------------------------------------------===//
struct SqrtOperator {
	template <class TA, class TR> static inline TR Operation(TA left) {
		return sqrt(left);
	}
};

void SqrtFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("sqrt", {LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                               UnaryDoubleFunctionWrapper<double, SqrtOperator>));
}

//===--------------------------------------------------------------------===//
// cbrt
//===--------------------------------------------------------------------===//
struct CbRtOperator {
	template <class TA, class TR> static inline TR Operation(TA left) {
		return cbrt(left);
	}
};

void CbrtFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("cbrt", {LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                               UnaryDoubleFunctionWrapper<double, CbRtOperator>));
}

//===--------------------------------------------------------------------===//
// ln
//===--------------------------------------------------------------------===//

struct LnOperator {
	template <class TA, class TR> static inline TR Operation(TA left) {
		return log(left);
	}
};

void LnFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("ln", {LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                               UnaryDoubleFunctionWrapper<double, LnOperator>));
}

//===--------------------------------------------------------------------===//
// log
//===--------------------------------------------------------------------===//
struct Log10Operator {
	template <class TA, class TR> static inline TR Operation(TA left) {
		return log10(left);
	}
};

void Log10Fun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction({"log10", "log"}, ScalarFunction({LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                                                 UnaryDoubleFunctionWrapper<double, Log10Operator>));
}

//===--------------------------------------------------------------------===//
// log2
//===--------------------------------------------------------------------===//
struct Log2Operator {
	template <class TA, class TR> static inline TR Operation(TA left) {
		return log2(left);
	}
};

void Log2Fun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("log2", {LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                               UnaryDoubleFunctionWrapper<double, Log2Operator>));
}

//===--------------------------------------------------------------------===//
// pi
//===--------------------------------------------------------------------===//
static void pi_function(DataChunk &args, ExpressionState &state, Vector &result) {
	assert(args.column_count() == 0);
	Value pi_value = Value::DOUBLE(PI);
	result.Reference(pi_value);
}

void PiFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("pi", {}, LogicalType::DOUBLE, pi_function));
}

//===--------------------------------------------------------------------===//
// degrees
//===--------------------------------------------------------------------===//
struct DegreesOperator {
	template <class TA, class TR> static inline TR Operation(TA left) {
		return left * (180 / PI);
	}
};

void DegreesFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("degrees", {LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                               UnaryDoubleFunctionWrapper<double, DegreesOperator>));
}

//===--------------------------------------------------------------------===//
// radians
//===--------------------------------------------------------------------===//
struct RadiansOperator {
	template <class TA, class TR> static inline TR Operation(TA left) {
		return left * (PI / 180);
	}
};

void RadiansFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("radians", {LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                               UnaryDoubleFunctionWrapper<double, RadiansOperator>));
}

//===--------------------------------------------------------------------===//
// sin
//===--------------------------------------------------------------------===//
struct SinOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		return sin(input);
	}
};

void SinFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("sin", {LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                               UnaryDoubleFunctionWrapper<double, SinOperator>));
}

//===--------------------------------------------------------------------===//
// cos
//===--------------------------------------------------------------------===//
struct CosOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		return (double)cos(input);
	}
};

void CosFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("cos", {LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                               UnaryDoubleFunctionWrapper<double, CosOperator>));
}

//===--------------------------------------------------------------------===//
// tan
//===--------------------------------------------------------------------===//
struct TanOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		return (double)tan(input);
	}
};

void TanFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("tan", {LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                               UnaryDoubleFunctionWrapper<double, TanOperator>));
}

//===--------------------------------------------------------------------===//
// asin
//===--------------------------------------------------------------------===//
struct ASinOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		if (input < -1 || input > 1) {
			throw Exception("ASIN is undefined outside [-1,1]");
		}
		return (double)asin(input);
	}
};

void AsinFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("asin", {LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                               UnaryDoubleFunctionWrapper<double, ASinOperator>));
}

//===--------------------------------------------------------------------===//
// atan
//===--------------------------------------------------------------------===//
struct ATanOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		return (double)atan(input);
	}
};

void AtanFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("atan", {LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                               UnaryDoubleFunctionWrapper<double, ATanOperator>));
}

//===--------------------------------------------------------------------===//
// atan2
//===--------------------------------------------------------------------===//
struct ATan2 {
	template <class TA, class TB, class TR> static inline TR Operation(TA left, TB right) {
		return (double)atan2(left, right);
	}
};

void Atan2Fun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("atan2", {LogicalType::DOUBLE, LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                               BinaryDoubleFunctionWrapper<double, ATan2>));
}

//===--------------------------------------------------------------------===//
// acos
//===--------------------------------------------------------------------===//
struct ACos {
	template <class TA, class TR> static inline TR Operation(TA input) {
		return (double)acos(input);
	}
};

void AcosFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(
	    ScalarFunction("acos", {LogicalType::DOUBLE}, LogicalType::DOUBLE, UnaryDoubleFunctionWrapper<double, ACos>));
}

//===--------------------------------------------------------------------===//
// cot
//===--------------------------------------------------------------------===//
struct CotOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		return 1.0 / (double)tan(input);
	}
};

void CotFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("cot", {LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                               UnaryDoubleFunctionWrapper<double, CotOperator>));
}

} // namespace duckdb

#include "duckdb/function/scalar/math_functions.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/scalar/trigonometric_functions.hpp"
#include "duckdb/common/types/hugeint.hpp"

#include "duckdb/common/algorithm.hpp"
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

void AbsFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet abs("abs");
	for (auto &type : LogicalType::NUMERIC) {
		abs.AddFunction(ScalarFunction({type}, type, ScalarFunction::GetScalarUnaryFunction<AbsOperator>(type)));
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
	functions.AddFunction(ScalarFunction({LogicalType::TINYINT}, LogicalType::TINYINT, ScalarFunction::UnaryFunction<int8_t, int8_t, BitCntOperator>));
	functions.AddFunction(ScalarFunction({LogicalType::SMALLINT}, LogicalType::TINYINT, ScalarFunction::UnaryFunction<int16_t, int8_t, BitCntOperator>));
	functions.AddFunction(ScalarFunction({LogicalType::INTEGER}, LogicalType::TINYINT, ScalarFunction::UnaryFunction<int32_t, int8_t, BitCntOperator>));
	functions.AddFunction(ScalarFunction({LogicalType::BIGINT}, LogicalType::TINYINT, ScalarFunction::UnaryFunction<int64_t, int8_t, BitCntOperator>));
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
		sign.AddFunction(ScalarFunction({type}, LogicalType::TINYINT,
		                                ScalarFunction::GetScalarUnaryFunctionFixedReturn<int8_t, SignOperator>(type)));
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

void CeilFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet ceil("ceil");
	for (auto &type : LogicalType::NUMERIC) {
		scalar_function_t func;
		if (type.IsIntegral()) {
			// ceil on integral type is a nop
			func = ScalarFunction::NopFunction;
		} else if (type.id() == LogicalTypeId::FLOAT) {
			func = ScalarFunction::UnaryFunction<float, float, CeilOperator>;
		} else if (type.id() == LogicalTypeId::DOUBLE) {
			func = ScalarFunction::UnaryFunction<double, double, CeilOperator>;
		} else {
			throw NotImplementedException("Unimplemented numeric type for function \"ceil\"");
		}
		ceil.AddFunction(ScalarFunction({type}, type, func));
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

void FloorFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet floor("floor");
	for (auto &type : LogicalType::NUMERIC) {
		scalar_function_t func;
		if (type.IsIntegral()) {
			// floor on integral type is a nop
			func = ScalarFunction::NopFunction;
		} else if (type.id() == LogicalTypeId::FLOAT) {
			func = ScalarFunction::UnaryFunction<float, float, FloorOperator>;
		} else if (type.id() == LogicalTypeId::DOUBLE) {
			func = ScalarFunction::UnaryFunction<double, double, FloorOperator>;
		} else {
			throw NotImplementedException("Unimplemented numeric type for function \"floor\"");
		}
		floor.AddFunction(ScalarFunction({type}, type, func));
	}
	set.AddFunction(floor);
}

//===--------------------------------------------------------------------===//
// round
//===--------------------------------------------------------------------===//
struct RoundOperator {
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

void RoundFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet round("round");
	for (auto &type : LogicalType::NUMERIC) {
		scalar_function_t func;
		if (type.IsIntegral()) {
			// round on integral type is a nop
			func = ScalarFunction::NopFunction;
		} else if (type.id() == LogicalTypeId::FLOAT) {
			func = ScalarFunction::BinaryFunction<float, int32_t, float, RoundOperator>;
		} else if (type.id() == LogicalTypeId::DOUBLE) {
			func = ScalarFunction::BinaryFunction<double, int32_t, double, RoundOperator>;
		} else {
			throw NotImplementedException("Unimplemented numeric type for function \"round\"");
		}
		round.AddFunction(ScalarFunction({type, LogicalType::INTEGER}, type, func));
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
	set.AddFunction(
	    ScalarFunction("exp", {LogicalType::DOUBLE}, LogicalType::DOUBLE, UnaryDoubleFunctionWrapper<double, ExpOperator>));
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
	set.AddFunction(
	    ScalarFunction("sqrt", {LogicalType::DOUBLE}, LogicalType::DOUBLE, UnaryDoubleFunctionWrapper<double, SqrtOperator>));
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
	set.AddFunction(
	    ScalarFunction("cbrt", {LogicalType::DOUBLE}, LogicalType::DOUBLE, UnaryDoubleFunctionWrapper<double, CbRtOperator>));
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
	set.AddFunction(
	    ScalarFunction("ln", {LogicalType::DOUBLE}, LogicalType::DOUBLE, UnaryDoubleFunctionWrapper<double, LnOperator>));
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
	set.AddFunction(
	    ScalarFunction("log2", {LogicalType::DOUBLE}, LogicalType::DOUBLE, UnaryDoubleFunctionWrapper<double, Log2Operator>));
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
	set.AddFunction(
	    ScalarFunction("sin", {LogicalType::DOUBLE}, LogicalType::DOUBLE, UnaryDoubleFunctionWrapper<double, SinOperator>));
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
	set.AddFunction(
	    ScalarFunction("cos", {LogicalType::DOUBLE}, LogicalType::DOUBLE, UnaryDoubleFunctionWrapper<double, CosOperator>));
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
	set.AddFunction(
	    ScalarFunction("tan", {LogicalType::DOUBLE}, LogicalType::DOUBLE, UnaryDoubleFunctionWrapper<double, TanOperator>));
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
	set.AddFunction(
	    ScalarFunction("asin", {LogicalType::DOUBLE}, LogicalType::DOUBLE, UnaryDoubleFunctionWrapper<double, ASinOperator>));
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
	set.AddFunction(
	    ScalarFunction("atan", {LogicalType::DOUBLE}, LogicalType::DOUBLE, UnaryDoubleFunctionWrapper<double, ATanOperator>));
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
	set.AddFunction(
	    ScalarFunction("cot", {LogicalType::DOUBLE}, LogicalType::DOUBLE, UnaryDoubleFunctionWrapper<double, CotOperator>));
}

} // namespace duckdb

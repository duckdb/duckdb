#include "duckdb/function/scalar/math_functions.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/scalar/trigonometric_functions.hpp"

#include <algorithm>
#include <cmath>

using namespace std;

namespace duckdb {

template <class TR, class OP> static scalar_function_t GetScalarIntegerUnaryFunctionFixedReturn(SQLType type) {
	switch (type.id) {
	case SQLTypeId::TINYINT:
		return ScalarFunction::UnaryFunction<int8_t, TR, OP>;
	case SQLTypeId::SMALLINT:
		return ScalarFunction::UnaryFunction<int16_t, TR, OP>;
	case SQLTypeId::INTEGER:
		return ScalarFunction::UnaryFunction<int32_t, TR, OP>;
	case SQLTypeId::BIGINT:
		return ScalarFunction::UnaryFunction<int64_t, TR, OP>;
	default:
		throw NotImplementedException("Unimplemented type for GetScalarIntegerUnaryFunctionFixedReturn");
	}
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
	template <class TA, class TR> static inline TR Operation(TA left) {
		return left < 0 ? left * -1 : left;
	}
};

void AbsFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet abs("abs");
	for (auto &type : SQLType::NUMERIC) {
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
	for (auto &type : SQLType::INTEGRAL) {
		functions.AddFunction(ScalarFunction({type}, SQLType::TINYINT,
		                                     GetScalarIntegerUnaryFunctionFixedReturn<int8_t, BitCntOperator>(type)));
	}
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
	for (auto &type : SQLType::NUMERIC) {
		sign.AddFunction(ScalarFunction({type}, SQLType::TINYINT,
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
	for (auto &type : SQLType::NUMERIC) {
		scalar_function_t func;
		if (type.IsIntegral()) {
			// ceil on integral type is a nop
			func = ScalarFunction::NopFunction;
		} else {
			func = ScalarFunction::GetScalarUnaryFunction<CeilOperator>(type);
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
	for (auto &type : SQLType::NUMERIC) {
		scalar_function_t func;
		if (type.IsIntegral()) {
			// floor on integral type is a nop
			func = ScalarFunction::NopFunction;
		} else {
			func = ScalarFunction::GetScalarUnaryFunction<FloorOperator>(type);
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
	for (auto &type : SQLType::NUMERIC) {
		scalar_function_t func;
		if (type.IsIntegral()) {
			// round on integral type is a nop
			func = ScalarFunction::NopFunction;
		} else if (type.id == SQLTypeId::FLOAT) {
			func = ScalarFunction::BinaryFunction<float, int32_t, float, RoundOperator>;
		} else {
			assert(type.id == SQLTypeId::DOUBLE || type.id == SQLTypeId::DECIMAL);
			func = ScalarFunction::BinaryFunction<double, int32_t, double, RoundOperator>;
		}
		round.AddFunction(ScalarFunction({type, SQLType::INTEGER}, type, func));
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
	    ScalarFunction("exp", {SQLType::DOUBLE}, SQLType::DOUBLE, UnaryDoubleFunctionWrapper<double, ExpOperator>));
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
	ScalarFunction power_function("pow", {SQLType::DOUBLE, SQLType::DOUBLE}, SQLType::DOUBLE,
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
	    ScalarFunction("sqrt", {SQLType::DOUBLE}, SQLType::DOUBLE, UnaryDoubleFunctionWrapper<double, SqrtOperator>));
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
	    ScalarFunction("cbrt", {SQLType::DOUBLE}, SQLType::DOUBLE, UnaryDoubleFunctionWrapper<double, CbRtOperator>));
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
	    ScalarFunction("ln", {SQLType::DOUBLE}, SQLType::DOUBLE, UnaryDoubleFunctionWrapper<double, LnOperator>));
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
	set.AddFunction({"log10", "log"}, ScalarFunction({SQLType::DOUBLE}, SQLType::DOUBLE,
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
	    ScalarFunction("log2", {SQLType::DOUBLE}, SQLType::DOUBLE, UnaryDoubleFunctionWrapper<double, Log2Operator>));
}

//===--------------------------------------------------------------------===//
// pi
//===--------------------------------------------------------------------===//
Value pi_value = Value::DOUBLE(PI);

static void pi_function(DataChunk &args, ExpressionState &state, Vector &result) {
	assert(args.column_count() == 0);
	result.Reference(pi_value);
}

void PiFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("pi", {}, SQLType::DOUBLE, pi_function));
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
	set.AddFunction(ScalarFunction("degrees", {SQLType::DOUBLE}, SQLType::DOUBLE,
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
	set.AddFunction(ScalarFunction("radians", {SQLType::DOUBLE}, SQLType::DOUBLE,
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
	    ScalarFunction("sin", {SQLType::DOUBLE}, SQLType::DOUBLE, UnaryDoubleFunctionWrapper<double, SinOperator>));
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
	    ScalarFunction("cos", {SQLType::DOUBLE}, SQLType::DOUBLE, UnaryDoubleFunctionWrapper<double, CosOperator>));
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
	    ScalarFunction("tan", {SQLType::DOUBLE}, SQLType::DOUBLE, UnaryDoubleFunctionWrapper<double, TanOperator>));
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
	    ScalarFunction("asin", {SQLType::DOUBLE}, SQLType::DOUBLE, UnaryDoubleFunctionWrapper<double, ASinOperator>));
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
	    ScalarFunction("atan", {SQLType::DOUBLE}, SQLType::DOUBLE, UnaryDoubleFunctionWrapper<double, ATanOperator>));
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
	set.AddFunction(ScalarFunction("atan2", {SQLType::DOUBLE, SQLType::DOUBLE}, SQLType::DOUBLE,
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
	    ScalarFunction("acos", {SQLType::DOUBLE}, SQLType::DOUBLE, UnaryDoubleFunctionWrapper<double, ACos>));
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
	    ScalarFunction("cot", {SQLType::DOUBLE}, SQLType::DOUBLE, UnaryDoubleFunctionWrapper<double, CotOperator>));
}

} // namespace duckdb

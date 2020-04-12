#include "duckdb/function/scalar/math_functions.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

#include <algorithm>
#include <cmath>

using namespace std;

namespace duckdb {

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
		TA modifier = 1;
		for(TB i = 0; i < precision; i++) {
			TA next = input * 10;
			TA next_modifier = modifier * 10;
			if (std::isinf(next) || std::isinf(next_modifier)) {
				return input;
			}
			modifier = next_modifier;
			input = next;
		}
		return round(input) / modifier;
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
	set.AddFunction(ScalarFunction("exp", {SQLType::DOUBLE}, SQLType::DOUBLE,
	                               ScalarFunction::UnaryFunction<double, double, ExpOperator>));
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
	                              ScalarFunction::BinaryFunction<double, double, double, PowOperator>);
	set.AddFunction(power_function);
	power_function.name = "power";
	set.AddFunction(power_function);
}

//===--------------------------------------------------------------------===//
// Unary wrappers to turn values < 0 or <= 0 into NULL
//===--------------------------------------------------------------------===//
struct UnaryNegativeWrapper {
	template <class FUNC, class OP, class INPUT_TYPE, class RESULT_TYPE>
	static inline RESULT_TYPE Operation(FUNC fun, INPUT_TYPE input, nullmask_t &nullmask, idx_t idx) {
		if (input < 0) {
			nullmask[idx] = true;
			return 0;
		} else {
			return OP::template Operation<INPUT_TYPE, RESULT_TYPE>(input);
		}
	}
};

struct UnaryZeroOrNegativeWrapper {
	template <class FUNC, class OP, class INPUT_TYPE, class RESULT_TYPE>
	static inline RESULT_TYPE Operation(FUNC fun, INPUT_TYPE input, nullmask_t &nullmask, idx_t idx) {
		if (input <= 0) {
			nullmask[idx] = true;
			return 0;
		} else {
			return OP::template Operation<INPUT_TYPE, RESULT_TYPE>(input);
		}
	}
};

template <class T, class OP, class OPWRAPPER>
static void UnaryScalarFunctionWrapper(DataChunk &input, ExpressionState &state, Vector &result) {
	UnaryExecutor::Execute<T, T, OP, true, OPWRAPPER>(input.data[0], result, input.size());
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
	set.AddFunction(ScalarFunction("sqrt", {SQLType::DOUBLE}, SQLType::DOUBLE,
	                               UnaryScalarFunctionWrapper<double, SqrtOperator, UnaryNegativeWrapper>));
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
	set.AddFunction(ScalarFunction("cbrt", {SQLType::DOUBLE}, SQLType::DOUBLE,
	                               ScalarFunction::UnaryFunction<double, double, CbRtOperator>));
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
	set.AddFunction(ScalarFunction("ln", {SQLType::DOUBLE}, SQLType::DOUBLE,
	                               UnaryScalarFunctionWrapper<double, LnOperator, UnaryZeroOrNegativeWrapper>));
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
	ScalarFunction log_function("log10", {SQLType::DOUBLE}, SQLType::DOUBLE,
	                               UnaryScalarFunctionWrapper<double, Log10Operator, UnaryZeroOrNegativeWrapper>);
	set.AddFunction(log_function);
	// "log" is an alias for "log10"
	log_function.name = "log";
	set.AddFunction(log_function);
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
	set.AddFunction(ScalarFunction("log2", {SQLType::DOUBLE}, SQLType::DOUBLE,
	                               UnaryScalarFunctionWrapper<double, Log2Operator, UnaryZeroOrNegativeWrapper>));
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
	                               ScalarFunction::UnaryFunction<double, double, DegreesOperator>));
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
	                               ScalarFunction::UnaryFunction<double, double, RadiansOperator>));
}

} // namespace duckdb

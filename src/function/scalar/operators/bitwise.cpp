#include "duckdb/function/scalar/operators.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

using namespace duckdb;
using namespace std;

namespace duckdb {

//===--------------------------------------------------------------------===//
// & [bitwise_and]
//===--------------------------------------------------------------------===//
struct BitwiseANDOperator {
	template <class TA, class TB, class TR> static inline TR Operation(TA left, TB right) {
		return left & right;
	}
};

void BitwiseAndFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet functions("&");
	for (auto &type : SQLType::INTEGRAL) {
		functions.AddFunction(ScalarFunction({type, type}, type,
		                                     ScalarFunction::GetScalarIntegerBinaryFunction<BitwiseANDOperator>(type)));
	}
	set.AddFunction(functions);
}

//===--------------------------------------------------------------------===//
// | [bitwise_or]
//===--------------------------------------------------------------------===//
struct BitwiseOROperator {
	template <class TA, class TB, class TR> static inline TR Operation(TA left, TB right) {
		return left | right;
	}
};

void BitwiseOrFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet functions("|");
	for (auto &type : SQLType::INTEGRAL) {
		functions.AddFunction(ScalarFunction({type, type}, type,
		                                     ScalarFunction::GetScalarIntegerBinaryFunction<BitwiseOROperator>(type)));
	}
	set.AddFunction(functions);
}

//===--------------------------------------------------------------------===//
// # [bitwise_xor]
//===--------------------------------------------------------------------===//
struct BitwiseXOROperator {
	template <class TA, class TB, class TR> static inline TR Operation(TA left, TB right) {
		return left ^ right;
	}
};

void BitwiseXorFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet functions("#");
	for (auto &type : SQLType::INTEGRAL) {
		functions.AddFunction(ScalarFunction({type, type}, type,
		                                     ScalarFunction::GetScalarIntegerBinaryFunction<BitwiseXOROperator>(type)));
	}
	set.AddFunction(functions);
}

//===--------------------------------------------------------------------===//
// << [bitwise_left_shift]
//===--------------------------------------------------------------------===//
template <class T> bool ShiftInRange(T shift) {
	return shift >= 0 && shift < (T)(sizeof(T) * 8);
}

struct BitwiseShiftLeftOperator {
	template <class TA, class TB, class TR> static inline TR Operation(TA input, TB shift) {
		return ShiftInRange(shift) ? input << shift : 0;
	}
};

void LeftShiftFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet functions("<<");
	for (auto &type : SQLType::INTEGRAL) {
		functions.AddFunction(ScalarFunction(
		    {type, type}, type, ScalarFunction::GetScalarIntegerBinaryFunction<BitwiseShiftLeftOperator>(type)));
	}
	set.AddFunction(functions);
}

//===--------------------------------------------------------------------===//
// >> [bitwise_right_shift]
//===--------------------------------------------------------------------===//
struct BitwiseShiftRightOperator {
	template <class TA, class TB, class TR> static inline TR Operation(TA input, TB shift) {
		return ShiftInRange(shift) ? input >> shift : 0;
	}
};

void RightShiftFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet functions(">>");
	for (auto &type : SQLType::INTEGRAL) {
		functions.AddFunction(ScalarFunction(
		    {type, type}, type, ScalarFunction::GetScalarIntegerBinaryFunction<BitwiseShiftRightOperator>(type)));
	}
	set.AddFunction(functions);
}

} // namespace duckdb

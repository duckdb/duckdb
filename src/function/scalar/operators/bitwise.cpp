#include "duckdb/function/scalar/operators.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

using namespace duckdb;
using namespace std;

namespace duckdb {

template <class OP> static scalar_function_t GetScalarIntegerBinaryFunction(SQLType type) {
	switch (type.id) {
	case SQLTypeId::TINYINT:
		return ScalarFunction::BinaryFunction<int8_t, int8_t, int8_t, OP>;
	case SQLTypeId::SMALLINT:
		return ScalarFunction::BinaryFunction<int16_t, int16_t, int16_t, OP>;
	case SQLTypeId::INTEGER:
		return ScalarFunction::BinaryFunction<int32_t, int32_t, int32_t, OP>;
	case SQLTypeId::BIGINT:
		return ScalarFunction::BinaryFunction<int64_t, int64_t, int64_t, OP>;
	default:
		throw NotImplementedException("Unimplemented type for GetScalarIntegerBinaryFunction");
	}
}

//===--------------------------------------------------------------------===//
// & [bitwise_and]
//===--------------------------------------------------------------------===//
struct BitwiseANDOperator {
	template <class T> static inline T Operation(T left, T right) {
		return left & right;
	}
};

void BitwiseAndFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet functions("&");
	for (auto &type : SQLType::INTEGRAL) {
		functions.AddFunction(
		    ScalarFunction({type, type}, type, GetScalarIntegerBinaryFunction<BitwiseANDOperator>(type)));
	}
	set.AddFunction(functions);
}

//===--------------------------------------------------------------------===//
// | [bitwise_or]
//===--------------------------------------------------------------------===//
struct BitwiseOROperator {
	template <class T> static inline T Operation(T left, T right) {
		return left | right;
	}
};

void BitwiseOrFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet functions("|");
	for (auto &type : SQLType::INTEGRAL) {
		functions.AddFunction(
		    ScalarFunction({type, type}, type, GetScalarIntegerBinaryFunction<BitwiseOROperator>(type)));
	}
	set.AddFunction(functions);
}

//===--------------------------------------------------------------------===//
// # [bitwise_xor]
//===--------------------------------------------------------------------===//
struct BitwiseXOROperator {
	template <class T> static inline T Operation(T left, T right) {
		return left ^ right;
	}
};

void BitwiseXorFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet functions("#");
	for (auto &type : SQLType::INTEGRAL) {
		functions.AddFunction(
		    ScalarFunction({type, type}, type, GetScalarIntegerBinaryFunction<BitwiseXOROperator>(type)));
	}
	set.AddFunction(functions);
}

//===--------------------------------------------------------------------===//
// << [bitwise_left_shift]
//===--------------------------------------------------------------------===//
struct BitwiseShiftLeftOperator {
	template <class T> static inline T Operation(T left, T right) {
		return left << right;
	}
};

void LeftShiftFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet functions("<<");
	for (auto &type : SQLType::INTEGRAL) {
		functions.AddFunction(
		    ScalarFunction({type, type}, type, GetScalarIntegerBinaryFunction<BitwiseShiftLeftOperator>(type)));
	}
	set.AddFunction(functions);
}

//===--------------------------------------------------------------------===//
// >> [bitwise_right_shift]
//===--------------------------------------------------------------------===//
struct BitwiseShiftRightOperator {
	template <class T> static inline T Operation(T left, T right) {
		return left >> right;
	}
};

void RightShiftFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet functions(">>");
	for (auto &type : SQLType::INTEGRAL) {
		functions.AddFunction(
		    ScalarFunction({type, type}, type, GetScalarIntegerBinaryFunction<BitwiseShiftRightOperator>(type)));
	}
	set.AddFunction(functions);
}

} // namespace duckdb

#include "duckdb/function/scalar/operators.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/operator/numeric_binary_operators.hpp"

using namespace std;

namespace duckdb {

template <class OP> static scalar_function_t GetScalarBinaryFunction(SQLType type) {
	switch (type.id) {
	case SQLTypeId::TINYINT:
		return ScalarFunction::BinaryFunction<int8_t, int8_t, int8_t, OP>;
	case SQLTypeId::SMALLINT:
		return ScalarFunction::BinaryFunction<int16_t, int16_t, int16_t, OP>;
	case SQLTypeId::INTEGER:
		return ScalarFunction::BinaryFunction<int32_t, int32_t, int32_t, OP>;
	case SQLTypeId::BIGINT:
		return ScalarFunction::BinaryFunction<int64_t, int64_t, int64_t, OP>;
	case SQLTypeId::FLOAT:
		return ScalarFunction::BinaryFunction<float, float, float, OP, true>;
	case SQLTypeId::DOUBLE:
		return ScalarFunction::BinaryFunction<double, double, double, OP, true>;
	case SQLTypeId::DECIMAL:
		return ScalarFunction::BinaryFunction<double, double, double, OP, true>;
	default:
		throw NotImplementedException("Unimplemented type for GetScalarBinaryFunction");
	}
}

//===--------------------------------------------------------------------===//
// + [add]
//===--------------------------------------------------------------------===//
template <> float AddOperator::Operation(float left, float right) {
	auto result = left + right;
	if (!Value::FloatIsValid(result)) {
		throw OutOfRangeException("Overflow in addition of float!");
	}
	return result;
}

template <> double AddOperator::Operation(double left, double right) {
	auto result = left + right;
	if (!Value::DoubleIsValid(result)) {
		throw OutOfRangeException("Overflow in addition of double!");
	}
	return result;
}

void AddFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet functions("+");
	// binary add function adds two numbers together
	for (auto &type : SQLType::NUMERIC) {
		functions.AddFunction(ScalarFunction({type, type}, type, GetScalarBinaryFunction<AddOperator>(type)));
	}
	// we can add integers to dates
	functions.AddFunction(ScalarFunction({SQLType::DATE, SQLType::INTEGER}, SQLType::DATE,
	                                     GetScalarBinaryFunction<AddOperator>(SQLType::INTEGER)));
	functions.AddFunction(ScalarFunction({SQLType::INTEGER, SQLType::DATE}, SQLType::DATE,
	                                     GetScalarBinaryFunction<AddOperator>(SQLType::INTEGER)));
	// unary add function is a nop, but only exists for numeric types
	for (auto &type : SQLType::NUMERIC) {
		functions.AddFunction(ScalarFunction({type}, type, ScalarFunction::NopFunction));
	}
	set.AddFunction(functions);
}

//===--------------------------------------------------------------------===//
// - [subtract]
//===--------------------------------------------------------------------===//
template <> float SubtractOperator::Operation(float left, float right) {
	auto result = left - right;
	if (!Value::FloatIsValid(result)) {
		throw OutOfRangeException("Overflow in subtraction of float!");
	}
	return result;
}

template <> double SubtractOperator::Operation(double left, double right) {
	auto result = left - right;
	if (!Value::DoubleIsValid(result)) {
		throw OutOfRangeException("Overflow in subtraction of double!");
	}
	return result;
}

void SubtractFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet functions("-");
	// binary subtract function "a - b", subtracts b from a
	for (auto &type : SQLType::NUMERIC) {
		functions.AddFunction(ScalarFunction({type, type}, type, GetScalarBinaryFunction<SubtractOperator>(type)));
	}
	functions.AddFunction(ScalarFunction({SQLType::DATE, SQLType::DATE}, SQLType::INTEGER,
	                                     GetScalarBinaryFunction<SubtractOperator>(SQLType::INTEGER)));
	functions.AddFunction(ScalarFunction({SQLType::DATE, SQLType::INTEGER}, SQLType::DATE,
	                                     GetScalarBinaryFunction<SubtractOperator>(SQLType::INTEGER)));
	// unary subtract function, negates the input (i.e. multiplies by -1)
	for (auto &type : SQLType::NUMERIC) {
		functions.AddFunction(
		    ScalarFunction({type}, type, ScalarFunction::GetScalarUnaryFunction<NegateOperator>(type)));
	}
	set.AddFunction(functions);
}

//===--------------------------------------------------------------------===//
// * [multiply]
//===--------------------------------------------------------------------===//
template <> float MultiplyOperator::Operation(float left, float right) {
	auto result = left * right;
	if (!Value::FloatIsValid(result)) {
		throw OutOfRangeException("Overflow in multiplication of float!");
	}
	return result;
}

template <> double MultiplyOperator::Operation(double left, double right) {
	auto result = left * right;
	if (!Value::DoubleIsValid(result)) {
		throw OutOfRangeException("Overflow in multiplication of double!");
	}
	return result;
}

void MultiplyFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet functions("*");
	for (auto &type : SQLType::NUMERIC) {
		functions.AddFunction(ScalarFunction({type, type}, type, GetScalarBinaryFunction<MultiplyOperator>(type)));
	}
	set.AddFunction(functions);
}

//===--------------------------------------------------------------------===//
// / [divide]
//===--------------------------------------------------------------------===//
template <> float DivideOperator::Operation(float left, float right) {
	auto result = left / right;
	if (!Value::FloatIsValid(result)) {
		throw OutOfRangeException("Overflow in division of float!");
	}
	return result;
}

template <> double DivideOperator::Operation(double left, double right) {
	auto result = left / right;
	if (!Value::DoubleIsValid(result)) {
		throw OutOfRangeException("Overflow in division of double!");
	}
	return result;
}

struct BinaryZeroIsNullWrapper {
	template <class FUNC, class OP, class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE>
	static inline RESULT_TYPE Operation(FUNC fun, LEFT_TYPE left, RIGHT_TYPE right, nullmask_t &nullmask, idx_t idx) {
		if (right == 0) {
			nullmask[idx] = true;
			return 0;
		} else {
			return OP::template Operation<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE>(left, right);
		}
	}
};

template <class T, class OP>
static void BinaryScalarFunctionIgnoreZero(DataChunk &input, ExpressionState &state, Vector &result) {
	BinaryExecutor::Execute<T, T, T, OP, true, BinaryZeroIsNullWrapper>(input.data[0], input.data[1], result,
	                                                                    input.size());
}

template <class OP> static scalar_function_t GetBinaryFunctionIgnoreZero(SQLType type) {
	switch (type.id) {
	case SQLTypeId::TINYINT:
		return BinaryScalarFunctionIgnoreZero<int8_t, OP>;
	case SQLTypeId::SMALLINT:
		return BinaryScalarFunctionIgnoreZero<int16_t, OP>;
	case SQLTypeId::INTEGER:
		return BinaryScalarFunctionIgnoreZero<int32_t, OP>;
	case SQLTypeId::BIGINT:
		return BinaryScalarFunctionIgnoreZero<int64_t, OP>;
	case SQLTypeId::FLOAT:
		return BinaryScalarFunctionIgnoreZero<float, OP>;
	case SQLTypeId::DOUBLE:
		return BinaryScalarFunctionIgnoreZero<double, OP>;
	case SQLTypeId::DECIMAL:
		return BinaryScalarFunctionIgnoreZero<double, OP>;
	default:
		throw NotImplementedException("Unimplemented type for GetScalarUnaryFunction");
	}
}

void DivideFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet functions("/");
	for (auto &type : SQLType::NUMERIC) {
		functions.AddFunction(ScalarFunction({type, type}, type, GetBinaryFunctionIgnoreZero<DivideOperator>(type)));
	}
	set.AddFunction(functions);
}

//===--------------------------------------------------------------------===//
// % [modulo]
//===--------------------------------------------------------------------===//
template <> float ModuloOperator::Operation(float left, float right) {
	assert(right != 0);
	return fmod(left, right);
}

template <> double ModuloOperator::Operation(double left, double right) {
	assert(right != 0);
	return fmod(left, right);
}

void ModFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet functions("%");
	for (auto &type : SQLType::NUMERIC) {
		functions.AddFunction(ScalarFunction({type, type}, type, GetBinaryFunctionIgnoreZero<ModuloOperator>(type)));
	}
	set.AddFunction(functions);
	functions.name = "mod";
	set.AddFunction(functions);
}

} // namespace duckdb

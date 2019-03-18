//===--------------------------------------------------------------------===//
// numeric_binary_operators.cpp
// Description: This file contains the implementation of the numeric binop
// operations (+ - / * %)
//===--------------------------------------------------------------------===//

#include "common/operator/numeric_binary_operators.hpp"

#include "common/vector_operations/binary_loops.hpp"
#include "common/vector_operations/vector_operations.hpp"

using namespace duckdb;
using namespace std;

//===--------------------------------------------------------------------===//
// Addition
//===--------------------------------------------------------------------===//
void VectorOperations::Add(Vector &left, Vector &right, Vector &result) {
	BINARY_TYPE_CHECK(left, right, result);
	switch (left.type) {
	case TypeId::TINYINT:
		templated_binary_loop<int8_t, int8_t, int8_t, operators::Add>(left, right, result);
		break;
	case TypeId::SMALLINT:
		templated_binary_loop<int16_t, int16_t, int16_t, operators::Add>(left, right, result);
		break;
	case TypeId::INTEGER:
		templated_binary_loop<int32_t, int32_t, int32_t, operators::Add>(left, right, result);
		break;
	case TypeId::BIGINT:
		templated_binary_loop<int64_t, int64_t, int64_t, operators::Add>(left, right, result);
		break;
	case TypeId::FLOAT:
		templated_binary_loop<float, float, float, operators::Add>(left, right, result);
		break;
	case TypeId::DOUBLE:
		templated_binary_loop<double, double, double, operators::Add>(left, right, result);
		break;
	case TypeId::POINTER:
		templated_binary_loop<uint64_t, uint64_t, uint64_t, operators::Add>(left, right, result);
		break;
	default:
		throw InvalidTypeException(left.type, "Invalid type for addition");
	}
}

//===--------------------------------------------------------------------===//
// Subtraction
//===--------------------------------------------------------------------===//
void VectorOperations::Subtract(Vector &left, Vector &right, Vector &result) {
	BINARY_TYPE_CHECK(left, right, result);
	switch (left.type) {
	case TypeId::TINYINT:
		templated_binary_loop<int8_t, int8_t, int8_t, operators::Subtract>(left, right, result);
		break;
	case TypeId::SMALLINT:
		templated_binary_loop<int16_t, int16_t, int16_t, operators::Subtract>(left, right, result);
		break;
	case TypeId::INTEGER:
		templated_binary_loop<int32_t, int32_t, int32_t, operators::Subtract>(left, right, result);
		break;
	case TypeId::BIGINT:
		templated_binary_loop<int64_t, int64_t, int64_t, operators::Subtract>(left, right, result);
		break;
	case TypeId::FLOAT:
		templated_binary_loop<float, float, float, operators::Subtract>(left, right, result);
		break;
	case TypeId::DOUBLE:
		templated_binary_loop<double, double, double, operators::Subtract>(left, right, result);
		break;
	case TypeId::POINTER:
		templated_binary_loop<uint64_t, uint64_t, uint64_t, operators::Subtract>(left, right, result);
		break;
	default:
		throw InvalidTypeException(left.type, "Invalid type for subtraction");
	}
}

//===--------------------------------------------------------------------===//
// Multiplication
//===--------------------------------------------------------------------===//
void VectorOperations::Multiply(Vector &left, Vector &right, Vector &result) {
	BINARY_TYPE_CHECK(left, right, result);
	switch (left.type) {
	case TypeId::TINYINT:
		templated_binary_loop<int8_t, int8_t, int8_t, operators::Multiply>(left, right, result);
		break;
	case TypeId::SMALLINT:
		templated_binary_loop<int16_t, int16_t, int16_t, operators::Multiply>(left, right, result);
		break;
	case TypeId::INTEGER:
		templated_binary_loop<int32_t, int32_t, int32_t, operators::Multiply>(left, right, result);
		break;
	case TypeId::BIGINT:
		templated_binary_loop<int64_t, int64_t, int64_t, operators::Multiply>(left, right, result);
		break;
	case TypeId::FLOAT:
		templated_binary_loop<float, float, float, operators::Multiply>(left, right, result);
		break;
	case TypeId::DOUBLE:
		templated_binary_loop<double, double, double, operators::Multiply>(left, right, result);
		break;
	case TypeId::POINTER:
		templated_binary_loop<uint64_t, uint64_t, uint64_t, operators::Multiply>(left, right, result);
		break;
	default:
		throw InvalidTypeException(left.type, "Invalid type for multiplication");
	}
}

//===--------------------------------------------------------------------===//
// Division
//===--------------------------------------------------------------------===//
// to handle (division by zero -> NULL) we have a separate function for handling
// division
template <class T> void templated_division_loop(Vector &left, Vector &right, Vector &result) {
	auto ldata = (T *)left.data;
	auto rdata = (T *)right.data;
	auto result_data = (T *)result.data;

	if (left.IsConstant()) {
		if (left.nullmask[0]) {
			// left side is constant NULL, set everything to NULL
			result.nullmask.set();
		} else {
			// left side is normal constant, use right nullmask and do
			// computation
			T constant = ldata[0];
			result.nullmask = right.nullmask;
			VectorOperations::Exec(right, [&](size_t i, size_t k) {
				if (rdata[i] == 0) {
					result.nullmask[i] = true;
				} else {
					result_data[i] = constant / rdata[i];
				}
			});
		}
		result.sel_vector = right.sel_vector;
		result.count = right.count;
	} else if (right.IsConstant()) {
		T constant = rdata[0];
		if (right.nullmask[0] || constant == 0) {
			// right side is constant NULL OR division by constant 0, set
			// everything to NULL
			result.nullmask.set();
		} else {
			// right side is normal constant, use left nullmask and do
			// computation
			result.nullmask = left.nullmask;
			binary_loop_function_right_constant<T, T, T, operators::Divide>(ldata, constant, result_data, left.count,
			                                                                left.sel_vector);
		}
		result.sel_vector = left.sel_vector;
		result.count = left.count;
	} else {
		assert(left.count == right.count);
		// OR nullmasks together
		result.nullmask = left.nullmask | right.nullmask;
		assert(left.sel_vector == right.sel_vector);
		VectorOperations::Exec(left, [&](size_t i, size_t k) {
			if (rdata[i] == 0) {
				result.nullmask[i] = true;
			} else {
				result_data[i] = ldata[i] / rdata[i];
			}
		});
		result.sel_vector = left.sel_vector;
		result.count = left.count;
	}
}

void VectorOperations::Divide(Vector &left, Vector &right, Vector &result) {
	BINARY_TYPE_CHECK(left, right, result);
	switch (left.type) {
	case TypeId::TINYINT:
		templated_division_loop<int8_t>(left, right, result);
		break;
	case TypeId::SMALLINT:
		templated_division_loop<int16_t>(left, right, result);
		break;
	case TypeId::INTEGER:
		templated_division_loop<int32_t>(left, right, result);
		break;
	case TypeId::BIGINT:
		templated_division_loop<int64_t>(left, right, result);
		break;
	case TypeId::FLOAT:
		templated_division_loop<float>(left, right, result);
		break;
	case TypeId::DOUBLE:
		templated_division_loop<double>(left, right, result);
		break;
	case TypeId::POINTER:
		templated_division_loop<uint64_t>(left, right, result);
		break;
	default:
		throw InvalidTypeException(left.type, "Invalid type for division");
	}
}

//===--------------------------------------------------------------------===//
// Modulo
//===--------------------------------------------------------------------===//
void VectorOperations::Modulo(Vector &left, Vector &right, Vector &result) {
	BINARY_TYPE_CHECK(left, right, result);
	switch (left.type) {
	case TypeId::TINYINT:
		templated_binary_loop<int8_t, int8_t, int8_t, operators::Modulo>(left, right, result);
		break;
	case TypeId::SMALLINT:
		templated_binary_loop<int16_t, int16_t, int16_t, operators::Modulo>(left, right, result);
		break;
	case TypeId::INTEGER:
		templated_binary_loop<int32_t, int32_t, int32_t, operators::Modulo>(left, right, result);
		break;
	case TypeId::BIGINT:
		templated_binary_loop<int64_t, int64_t, int64_t, operators::Modulo>(left, right, result);
		break;
	case TypeId::POINTER:
		templated_binary_loop<uint64_t, uint64_t, uint64_t, operators::Modulo>(left, right, result);
		break;
	default:
		throw InvalidTypeException(left.type, "Invalid type for modulo");
	}
}

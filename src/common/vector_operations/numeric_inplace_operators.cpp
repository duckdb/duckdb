//===--------------------------------------------------------------------===//
// numeric_inplace_operators.cpp
// Description: This file contains the implementation of numeric inplace ops
// += *= /= -= %=
//===--------------------------------------------------------------------===//

#include "common/operator/numeric_inplace_operators.hpp"

#include "common/types/constant_vector.hpp"
#include "common/vector_operations/inplace_loops.hpp"
#include "common/vector_operations/vector_operations.hpp"

using namespace duckdb;
using namespace std;

//===--------------------------------------------------------------------===//
// In-Place Addition
//===--------------------------------------------------------------------===//
// left += right
void VectorOperations::AddInPlace(Vector &result, Vector &input) {
	INPLACE_TYPE_CHECK(input, result);
	// the inplace loops take the result as the last parameter
	switch (input.type) {
	case TypeId::TINYINT:
		templated_inplace_loop<int8_t, int8_t, operators::AddInPlace>(input, result);
		break;
	case TypeId::SMALLINT:
		templated_inplace_loop<int16_t, int16_t, operators::AddInPlace>(input, result);
		break;
	case TypeId::INTEGER:
		templated_inplace_loop<int32_t, int32_t, operators::AddInPlace>(input, result);
		break;
	case TypeId::BIGINT:
		templated_inplace_loop<int64_t, int64_t, operators::AddInPlace>(input, result);
		break;
	case TypeId::DOUBLE:
		templated_inplace_loop<double, double, operators::AddInPlace>(input, result);
		break;
	case TypeId::POINTER:
		templated_inplace_loop<uint64_t, uint64_t, operators::AddInPlace>(input, result);
		break;
	default:
		throw InvalidTypeException(input.type, "Invalid type for addition");
	}
}

void VectorOperations::AddInPlace(Vector &left, int64_t right) {
	Value right_value = Value::Numeric(left.type, right);
	ConstantVector right_vector(right_value);
	VectorOperations::AddInPlace(left, right_vector);
}

//===--------------------------------------------------------------------===//
// In-Place Modulo
//===--------------------------------------------------------------------===//
// left += right
void VectorOperations::ModuloInPlace(Vector &result, Vector &input) {
	INPLACE_TYPE_CHECK(input, result);
	// the inplace loops take the result as the last parameter
	switch (input.type) {
	case TypeId::TINYINT:
		templated_inplace_loop<int8_t, int8_t, operators::ModuloInPlace>(input, result);
		break;
	case TypeId::SMALLINT:
		templated_inplace_loop<int16_t, int16_t, operators::ModuloInPlace>(input, result);
		break;
	case TypeId::INTEGER:
		templated_inplace_loop<int32_t, int32_t, operators::ModuloInPlace>(input, result);
		break;
	case TypeId::BIGINT:
		templated_inplace_loop<int64_t, int64_t, operators::ModuloInPlace>(input, result);
		break;
	case TypeId::POINTER:
		templated_inplace_loop<uint64_t, uint64_t, operators::ModuloInPlace>(input, result);
		break;
	default:
		throw InvalidTypeException(input.type, "Invalid type for addition");
	}
}

void VectorOperations::ModuloInPlace(Vector &left, int64_t right) {
	Value right_value = Value::Numeric(left.type, right);
	ConstantVector right_vector(right_value);
	VectorOperations::ModuloInPlace(left, right_vector);
}

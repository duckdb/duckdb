//===--------------------------------------------------------------------===//
// numeric_binary_operators.cpp
// Description: This file contains the implementation of the numeric binop
// operations (+ - / * %)
//===--------------------------------------------------------------------===//

#include "duckdb/common/operator/numeric_binary_operators.hpp"

#include "duckdb/common/vector_operations/binary_loops.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

using namespace duckdb;
using namespace std;

//===--------------------------------------------------------------------===//
// Addition
//===--------------------------------------------------------------------===//
void VectorOperations::Add(Vector &left, Vector &right, Vector &result) {
	BINARY_TYPE_CHECK(left, right, result);
	switch (left.type) {
	case TypeId::TINYINT:
		templated_binary_loop<int8_t, int8_t, int8_t, duckdb::Add>(left, right, result);
		break;
	case TypeId::SMALLINT:
		templated_binary_loop<int16_t, int16_t, int16_t, duckdb::Add>(left, right, result);
		break;
	case TypeId::INTEGER:
		templated_binary_loop<int32_t, int32_t, int32_t, duckdb::Add>(left, right, result);
		break;
	case TypeId::BIGINT:
		templated_binary_loop<int64_t, int64_t, int64_t, duckdb::Add>(left, right, result);
		break;
	case TypeId::FLOAT:
		templated_binary_loop<float, float, float, duckdb::Add>(left, right, result);
		break;
	case TypeId::DOUBLE:
		templated_binary_loop<double, double, double, duckdb::Add>(left, right, result);
		break;
	case TypeId::POINTER:
		templated_binary_loop<uint64_t, uint64_t, uint64_t, duckdb::Add>(left, right, result);
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
		templated_binary_loop<int8_t, int8_t, int8_t, duckdb::Subtract>(left, right, result);
		break;
	case TypeId::SMALLINT:
		templated_binary_loop<int16_t, int16_t, int16_t, duckdb::Subtract>(left, right, result);
		break;
	case TypeId::INTEGER:
		templated_binary_loop<int32_t, int32_t, int32_t, duckdb::Subtract>(left, right, result);
		break;
	case TypeId::BIGINT:
		templated_binary_loop<int64_t, int64_t, int64_t, duckdb::Subtract>(left, right, result);
		break;
	case TypeId::FLOAT:
		templated_binary_loop<float, float, float, duckdb::Subtract>(left, right, result);
		break;
	case TypeId::DOUBLE:
		templated_binary_loop<double, double, double, duckdb::Subtract>(left, right, result);
		break;
	case TypeId::POINTER:
		templated_binary_loop<uint64_t, uint64_t, uint64_t, duckdb::Subtract>(left, right, result);
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
		templated_binary_loop<int8_t, int8_t, int8_t, duckdb::Multiply>(left, right, result);
		break;
	case TypeId::SMALLINT:
		templated_binary_loop<int16_t, int16_t, int16_t, duckdb::Multiply>(left, right, result);
		break;
	case TypeId::INTEGER:
		templated_binary_loop<int32_t, int32_t, int32_t, duckdb::Multiply>(left, right, result);
		break;
	case TypeId::BIGINT:
		templated_binary_loop<int64_t, int64_t, int64_t, duckdb::Multiply>(left, right, result);
		break;
	case TypeId::FLOAT:
		templated_binary_loop<float, float, float, duckdb::Multiply>(left, right, result);
		break;
	case TypeId::DOUBLE:
		templated_binary_loop<double, double, double, duckdb::Multiply>(left, right, result);
		break;
	case TypeId::POINTER:
		templated_binary_loop<uint64_t, uint64_t, uint64_t, duckdb::Multiply>(left, right, result);
		break;
	default:
		throw InvalidTypeException(left.type, "Invalid type for multiplication");
	}
}

void VectorOperations::Divide(Vector &left, Vector &right, Vector &result) {
	BINARY_TYPE_CHECK(left, right, result);
	switch (left.type) {
	case TypeId::TINYINT:
		templated_divmod_loop<int8_t, duckdb::Divide>(left, right, result);
		break;
	case TypeId::SMALLINT:
		templated_divmod_loop<int16_t, duckdb::Divide>(left, right, result);
		break;
	case TypeId::INTEGER:
		templated_divmod_loop<int32_t, duckdb::Divide>(left, right, result);
		break;
	case TypeId::BIGINT:
		templated_divmod_loop<int64_t, duckdb::Divide>(left, right, result);
		break;
	case TypeId::FLOAT:
		templated_divmod_loop<float, duckdb::Divide>(left, right, result);
		break;
	case TypeId::DOUBLE:
		templated_divmod_loop<double, duckdb::Divide>(left, right, result);
		break;
	case TypeId::POINTER:
		templated_divmod_loop<uint64_t, duckdb::Divide>(left, right, result);
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
		templated_divmod_loop<int8_t, duckdb::ModuloInt>(left, right, result);
		break;
	case TypeId::SMALLINT:
		templated_divmod_loop<int16_t, duckdb::ModuloInt>(left, right, result);
		break;
	case TypeId::INTEGER:
		templated_divmod_loop<int32_t, duckdb::ModuloInt>(left, right, result);
		break;
	case TypeId::BIGINT:
		templated_divmod_loop<int64_t, duckdb::ModuloInt>(left, right, result);
		break;
	case TypeId::FLOAT:
		templated_divmod_loop<float, duckdb::ModuloReal>(left, right, result);
		break;
	case TypeId::DOUBLE:
		templated_divmod_loop<double, duckdb::ModuloReal>(left, right, result);
		break;
	case TypeId::POINTER:
		templated_divmod_loop<uint64_t, duckdb::ModuloInt>(left, right, result);
		break;
	default:
		throw InvalidTypeException(left.type, "Invalid type for ModuloInt");
	}
}

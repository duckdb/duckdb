//===--------------------------------------------------------------------===//
// numeric_bitwise_operators.cpp
// Description: This file contains the implementation of the numeric bitwise
// operations (^ | & >> << ~)
//===--------------------------------------------------------------------===//

#include "duckdb/common/operator/numeric_bitwise_operators.hpp"

#include "duckdb/common/vector_operations/binary_loops.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

using namespace duckdb;
using namespace std;

template <class OP> static void templated_binary_bitwise_operation(Vector &left, Vector &right, Vector &result) {
	BINARY_TYPE_CHECK(left, right, result);
	switch (left.type) {
	case TypeId::TINYINT:
		templated_binary_loop<int8_t, int8_t, int8_t, OP>(left, right, result);
		break;
	case TypeId::SMALLINT:
		templated_binary_loop<int16_t, int16_t, int16_t, OP>(left, right, result);
		break;
	case TypeId::INTEGER:
		templated_binary_loop<int32_t, int32_t, int32_t, OP>(left, right, result);
		break;
	case TypeId::BIGINT:
		templated_binary_loop<int64_t, int64_t, int64_t, OP>(left, right, result);
		break;
	case TypeId::POINTER:
		templated_binary_loop<uint64_t, uint64_t, uint64_t, OP>(left, right, result);
		break;
	default:
		throw InvalidTypeException(left.type, "Invalid type for bitwise operation");
	}
}

//===--------------------------------------------------------------------===//
// Bitwise XOR
//===--------------------------------------------------------------------===//
void VectorOperations::BitwiseXOR(Vector &left, Vector &right, Vector &result) {
	templated_binary_bitwise_operation<duckdb::BitwiseXOR>(left, right, result);
}

//===--------------------------------------------------------------------===//
// Bitwise AND
//===--------------------------------------------------------------------===//
void VectorOperations::BitwiseAND(Vector &left, Vector &right, Vector &result) {
	templated_binary_bitwise_operation<duckdb::BitwiseAND>(left, right, result);
}

//===--------------------------------------------------------------------===//
// Bitwise OR
//===--------------------------------------------------------------------===//
void VectorOperations::BitwiseOR(Vector &left, Vector &right, Vector &result) {
	templated_binary_bitwise_operation<duckdb::BitwiseOR>(left, right, result);
}

//===--------------------------------------------------------------------===//
// Bitwise Shift Left
//===--------------------------------------------------------------------===//
void VectorOperations::BitwiseShiftLeft(Vector &left, Vector &right, Vector &result) {
	templated_binary_bitwise_operation<duckdb::BitwiseShiftLeft>(left, right, result);
}

//===--------------------------------------------------------------------===//
// Bitwise Shift Right
//===--------------------------------------------------------------------===//
void VectorOperations::BitwiseShiftRight(Vector &left, Vector &right, Vector &result) {
	templated_binary_bitwise_operation<duckdb::BitwiseShiftRight>(left, right, result);
}

//===--------------------------------------------------------------------===//
// hash.cpp
// Description: This file contains the vectorized hash implementations
//===--------------------------------------------------------------------===//

#include "common/operator/hash_operators.hpp"
#include "common/vector_operations/binary_loops.hpp"
#include "common/vector_operations/unary_loops.hpp"

using namespace duckdb;
using namespace std;

void VectorOperations::Hash(Vector &input, Vector &result) {
	if (result.type != TypeId::INTEGER) {
		throw InvalidTypeException(result.type,
		                           "result of hash must be an integer");
	}
	switch (input.type) {
	case TypeId::BOOLEAN:
	case TypeId::TINYINT:
		templated_unary_loop_process_null<int8_t, int32_t, operators::Hash>(
		    input, result);
		break;
	case TypeId::SMALLINT:
		templated_unary_loop_process_null<int16_t, int32_t, operators::Hash>(
		    input, result);
		break;
	case TypeId::DATE:
	case TypeId::INTEGER:
		templated_unary_loop_process_null<int32_t, int32_t, operators::Hash>(
		    input, result);
		break;
	case TypeId::TIMESTAMP:
	case TypeId::BIGINT:
		templated_unary_loop_process_null<int64_t, int32_t, operators::Hash>(
		    input, result);
		break;
	case TypeId::POINTER:
		templated_unary_loop_process_null<uint64_t, int32_t, operators::Hash>(
		    input, result);
		break;
	case TypeId::DECIMAL:
		templated_unary_loop_process_null<double, int32_t, operators::Hash>(
		    input, result);
		break;
	case TypeId::VARCHAR:
		templated_unary_loop_process_null<const char *, int32_t,
		                                  operators::Hash>(input, result);
		break;
	default:
		throw InvalidTypeException(input.type, "Invalid type for hash");
	}
}

void VectorOperations::CombineHash(Vector &hashes, Vector &input) {
	if (hashes.type != TypeId::INTEGER) {
		throw NotImplementedException(
		    "Hashes must be 32-bit integer hash vector");
	}
	// first hash the input to an intermediate vector
	Vector intermediate(TypeId::INTEGER, true, false);
	VectorOperations::Hash(input, intermediate);
	// then XOR it together with the input
	VectorOperations::BitwiseXORInPlace(hashes, intermediate);
}
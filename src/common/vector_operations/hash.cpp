//===--------------------------------------------------------------------===//
// hash.cpp
// Description: This file contains the vectorized hash implementations
//===--------------------------------------------------------------------===//

#include "duckdb/common/operator/hash_operators.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

using namespace duckdb;
using namespace std;

template <class LEFT_TYPE, class RESULT_TYPE, class OP>
static inline void unary_loop_process_null_function(LEFT_TYPE *__restrict ldata, RESULT_TYPE *__restrict result_data,
                                                    index_t count, sel_t *__restrict sel_vector, nullmask_t &nullmask) {
	ASSERT_RESTRICT(ldata, ldata + count, result_data, result_data + count);
	if (nullmask.any()) {
		VectorOperations::Exec(sel_vector, count,
		                       [&](index_t i, index_t k) { result_data[i] = OP::Operation(ldata[i], nullmask[i]); });
	} else {
		VectorOperations::Exec(sel_vector, count,
		                       [&](index_t i, index_t k) { result_data[i] = OP::Operation(ldata[i], false); });
	}
}

template <class LEFT_TYPE, class RESULT_TYPE, class OP>
void templated_unary_loop_process_null(Vector &input, Vector &result) {
	auto ldata = (LEFT_TYPE *)input.data;
	auto result_data = (RESULT_TYPE *)result.data;

	result.nullmask.reset();
	unary_loop_process_null_function<LEFT_TYPE, RESULT_TYPE, OP>(ldata, result_data, input.count, input.sel_vector,
	                                                             input.nullmask);
	result.sel_vector = input.sel_vector;
	result.count = input.count;
}

void VectorOperations::Hash(Vector &input, Vector &result) {
	if (result.type != TypeId::HASH) {
		throw InvalidTypeException(result.type, "result of hash must be a uint64_t");
	}
	switch (input.type) {
	case TypeId::BOOLEAN:
	case TypeId::TINYINT:
		templated_unary_loop_process_null<int8_t, uint64_t, duckdb::HashOp>(input, result);
		break;
	case TypeId::SMALLINT:
		templated_unary_loop_process_null<int16_t, uint64_t, duckdb::HashOp>(input, result);
		break;
	case TypeId::INTEGER:
		templated_unary_loop_process_null<int32_t, uint64_t, duckdb::HashOp>(input, result);
		break;
	case TypeId::BIGINT:
		templated_unary_loop_process_null<int64_t, uint64_t, duckdb::HashOp>(input, result);
		break;
	case TypeId::FLOAT:
		templated_unary_loop_process_null<float, uint64_t, duckdb::HashOp>(input, result);
		break;
	case TypeId::DOUBLE:
		templated_unary_loop_process_null<double, uint64_t, duckdb::HashOp>(input, result);
		break;
	case TypeId::VARCHAR:
		templated_unary_loop_process_null<const char *, uint64_t, duckdb::HashOp>(input, result);
		break;
	default:
		throw InvalidTypeException(input.type, "Invalid type for hash");
	}
}

void VectorOperations::CombineHash(Vector &hashes, Vector &input) {
	if (hashes.type != TypeId::HASH) {
		throw NotImplementedException("Hashes must be 64-bit unsigned integer hash vector");
	}
	// first hash the input to an intermediate vector
	Vector intermediate(TypeId::HASH, true, false);
	VectorOperations::Hash(input, intermediate);
	// then XOR it together with the input
	VectorOperations::BitwiseXORInPlace(hashes, intermediate);
}

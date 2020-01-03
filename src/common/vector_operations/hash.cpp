//===--------------------------------------------------------------------===//
// hash.cpp
// Description: This file contains the vectorized hash implementations
//===--------------------------------------------------------------------===//

#include "duckdb/common/operator/hash_operators.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

using namespace duckdb;
using namespace std;

template <class T>
static inline void tight_loop_hash(T *__restrict ldata, uint64_t *__restrict result_data, index_t count,
                                   sel_t *__restrict sel_vector, nullmask_t &nullmask) {
	ASSERT_RESTRICT(ldata, ldata + count, result_data, result_data + count);
	if (nullmask.any()) {
		VectorOperations::Exec(sel_vector, count, [&](index_t i, index_t k) {
			result_data[i] = duckdb::HashOp::Operation(ldata[i], nullmask[i]);
		});
	} else {
		VectorOperations::Exec(sel_vector, count, [&](index_t i, index_t k) {
			result_data[i] = duckdb::HashOp::Operation(ldata[i], false);
		});
	}
}

template <class T> void templated_loop_hash(Vector &input, Vector &result) {
	auto ldata = (T *)input.data;
	auto result_data = (uint64_t *)result.data;

	result.nullmask.reset();
	tight_loop_hash<T>(ldata, result_data, input.count, input.sel_vector, input.nullmask);
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
		templated_loop_hash<int8_t>(input, result);
		break;
	case TypeId::SMALLINT:
		templated_loop_hash<int16_t>(input, result);
		break;
	case TypeId::INTEGER:
		templated_loop_hash<int32_t>(input, result);
		break;
	case TypeId::BIGINT:
		templated_loop_hash<int64_t>(input, result);
		break;
	case TypeId::FLOAT:
		templated_loop_hash<float>(input, result);
		break;
	case TypeId::DOUBLE:
		templated_loop_hash<double>(input, result);
		break;
	case TypeId::VARCHAR:
		templated_loop_hash<const char *>(input, result);
		break;
	default:
		throw InvalidTypeException(input.type, "Invalid type for hash");
	}
}

static inline uint64_t combine_hash(uint64_t a, uint64_t b) {
	return (a * UINT64_C(0xbf58476d1ce4e5b9)) ^ b;
}

template <class T>
static inline void tight_loop_combine_hash(T *__restrict ldata, uint64_t *__restrict hash_data, index_t count,
                                           sel_t *__restrict sel_vector, nullmask_t &nullmask) {
	ASSERT_RESTRICT(ldata, ldata + count, hash_data, hash_data + count);
	if (nullmask.any()) {
		VectorOperations::Exec(sel_vector, count, [&](index_t i, index_t k) {
			auto other_hash = duckdb::HashOp::Operation(ldata[i], nullmask[i]);
			hash_data[i] = combine_hash(hash_data[i], other_hash);
		});
	} else {
		VectorOperations::Exec(sel_vector, count, [&](index_t i, index_t k) {
			auto other_hash = duckdb::HashOp::Operation(ldata[i], false);
			hash_data[i] = combine_hash(hash_data[i], other_hash);
		});
	}
}

template <class T> void templated_loop_combine_hash(Vector &input, Vector &hashes) {
	auto ldata = (T *)input.data;
	auto hash_data = (uint64_t *)hashes.data;

	tight_loop_combine_hash<T>(ldata, hash_data, input.count, input.sel_vector, input.nullmask);
}

void VectorOperations::CombineHash(Vector &hashes, Vector &input) {
	if (hashes.type != TypeId::HASH) {
		throw NotImplementedException("Hashes must be 64-bit unsigned integer hash vector");
	}
	assert(input.sel_vector == hashes.sel_vector && input.count == hashes.count);
	switch (input.type) {
	case TypeId::BOOLEAN:
	case TypeId::TINYINT:
		templated_loop_combine_hash<int8_t>(input, hashes);
		break;
	case TypeId::SMALLINT:
		templated_loop_combine_hash<int16_t>(input, hashes);
		break;
	case TypeId::INTEGER:
		templated_loop_combine_hash<int32_t>(input, hashes);
		break;
	case TypeId::BIGINT:
		templated_loop_combine_hash<int64_t>(input, hashes);
		break;
	case TypeId::FLOAT:
		templated_loop_combine_hash<float>(input, hashes);
		break;
	case TypeId::DOUBLE:
		templated_loop_combine_hash<double>(input, hashes);
		break;
	case TypeId::VARCHAR:
		templated_loop_combine_hash<const char *>(input, hashes);
		break;
	default:
		throw InvalidTypeException(input.type, "Invalid type for hash");
	}
}

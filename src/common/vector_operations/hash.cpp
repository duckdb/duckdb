//===--------------------------------------------------------------------===//
// hash.cpp
// Description: This file contains the vectorized hash implementations
//===--------------------------------------------------------------------===//

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/types/null_value.hpp"

using namespace duckdb;
using namespace std;

struct HashOp {
	template <class T> static inline uint64_t Operation(T input, bool is_null) {
		return duckdb::Hash<T>(is_null ? duckdb::NullValue<T>() : input);
	}
};

template <class T>
static inline void tight_loop_hash(T *__restrict ldata, uint64_t *__restrict result_data, index_t count,
                                   sel_t *__restrict sel_vector, nullmask_t &nullmask) {
	ASSERT_RESTRICT(ldata, ldata + count, result_data, result_data + count);
	if (nullmask.any()) {
		VectorOperations::Exec(sel_vector, count, [&](index_t i, index_t k) {
			result_data[i] = HashOp::Operation(ldata[i], nullmask[i]);
		});
	} else {
		VectorOperations::Exec(sel_vector, count,
		                       [&](index_t i, index_t k) { result_data[i] = HashOp::Operation(ldata[i], false); });
	}
}

template <class T> void templated_loop_hash(Vector &input, Vector &result) {
	auto result_data = (uint64_t *)result.GetData();

	if (input.vector_type == VectorType::CONSTANT_VECTOR) {
		auto ldata = (T *)input.GetData();
		result.vector_type = VectorType::CONSTANT_VECTOR;
		result_data[0] = HashOp::Operation(ldata[0], input.nullmask[0]);
	} else {
		input.Normalify();
		auto ldata = (T *)input.GetData();
		result.vector_type = VectorType::FLAT_VECTOR;
		tight_loop_hash<T>(ldata, result_data, input.count, input.sel_vector, input.nullmask);
	}
}

void VectorOperations::Hash(Vector &input, Vector &result) {
	assert(result.type == TypeId::HASH);
	assert(!result.nullmask.any());
	result.sel_vector = input.sel_vector;
	result.count = input.count;
	switch (input.type) {
	case TypeId::BOOL:
	case TypeId::INT8:
		templated_loop_hash<int8_t>(input, result);
		break;
	case TypeId::INT16:
		templated_loop_hash<int16_t>(input, result);
		break;
	case TypeId::INT32:
		templated_loop_hash<int32_t>(input, result);
		break;
	case TypeId::INT64:
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
			auto other_hash = HashOp::Operation(ldata[i], nullmask[i]);
			hash_data[i] = combine_hash(hash_data[i], other_hash);
		});
	} else {
		VectorOperations::Exec(sel_vector, count, [&](index_t i, index_t k) {
			auto other_hash = HashOp::Operation(ldata[i], false);
			hash_data[i] = combine_hash(hash_data[i], other_hash);
		});
	}
}

template <class T> void templated_loop_combine_hash(Vector &input, Vector &hashes) {
	if (input.vector_type == VectorType::CONSTANT_VECTOR && hashes.vector_type == VectorType::CONSTANT_VECTOR) {
		auto ldata = (T *)input.GetData();
		auto hash_data = (uint64_t *)hashes.GetData();

		auto other_hash = HashOp::Operation(ldata[0], input.nullmask[0]);
		hash_data[0] = combine_hash(hash_data[0], other_hash);
	} else {
		input.Normalify();
		hashes.Normalify();
		tight_loop_combine_hash<T>((T *)input.GetData(), (uint64_t *)hashes.GetData(), input.count, input.sel_vector,
		                           input.nullmask);
	}
}

void VectorOperations::CombineHash(Vector &hashes, Vector &input) {
	assert(hashes.type == TypeId::HASH);
	assert(input.sel_vector == hashes.sel_vector && input.count == hashes.count);
	assert(!hashes.nullmask.any());
	switch (input.type) {
	case TypeId::BOOL:
	case TypeId::INT8:
		templated_loop_combine_hash<int8_t>(input, hashes);
		break;
	case TypeId::INT16:
		templated_loop_combine_hash<int16_t>(input, hashes);
		break;
	case TypeId::INT32:
		templated_loop_combine_hash<int32_t>(input, hashes);
		break;
	case TypeId::INT64:
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
